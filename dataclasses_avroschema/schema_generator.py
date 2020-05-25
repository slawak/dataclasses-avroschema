import dataclasses
import json
import typing
import collections
import copy

from dataclasses_avroschema.schema_definition import AvroSchemaDefinition
from dataclasses_avroschema.schemaless_avro_codec import get_schemaless_pathes, get_schemaless_avro_schema, SCHEMALESS_AVRO_SCHEMA


class SchemaGenerator:
    def __init__(self, klass_or_instance, include_schema_doc: bool = False) -> None:
        self.dataclass = self.generate_dataclass(klass_or_instance)
        self.include_schema_doc = include_schema_doc
        self.schema_definition: AvroSchemaDefinition = None

    @staticmethod
    def generate_dataclass(klass_or_instance):
        if dataclasses.is_dataclass(klass_or_instance):
            return klass_or_instance
        return dataclasses.dataclass(klass_or_instance)

    def generate_schema(self, schema_type: str = "avro"):
        if self.schema_definition is not None:
            return self.schema_definition.render()

        # let's live open the possibility to define different
        # schema definitions like json
        if schema_type == "avro":
            schema_definition = self._generate_avro_schema()
        else:
            raise ValueError("Invalid type. Expected avro schema type.")

        # cache the schema
        self.schema_definition = schema_definition

        return self.schema_definition.render()

    def _generate_avro_schema(self) -> AvroSchemaDefinition:
        return AvroSchemaDefinition(
            "record", self.dataclass, include_schema_doc=self.include_schema_doc
        )

    @property
    def get_fields(self) -> typing.List["Field"]:
        if self.schema_definition is None:
            self.generate_schema()

        return self.schema_definition.fields

    def get_schemaless_pathes(self) -> typing.List[typing.List[str]]:
        return get_schemaless_pathes(self.generate_schema(schema_type="avro"))

    def avro_schema(self, canonical: bool = False, faust_field: bool = False, namespace: str = None) -> str:
        schema = copy.deepcopy(self.generate_schema(schema_type="avro"))
        if canonical:
            pre_visit_func = _default_pre_visit_func
            post_visit_func = _default_post_visit_func
            if faust_field:
                post_visit_func = _faust_post_visit_func
            if namespace:
                pre_visit_func = _namespace_overriding_pre_visit_func
            schema = _schema_visitor(schema, {},
                                     namespace=namespace,
                                     pre_visit_func=pre_visit_func,
                                     post_visit_func=post_visit_func,
                                     )
        return json.dumps(schema)

    def avro_schema_to_python(self, canonical: bool = False, faust_field: bool = False, namespace: str = None) -> typing.Mapping[str, typing.Any]:
        return json.loads(self.avro_schema(canonical=canonical, faust_field=faust_field, namespace=namespace))


def _default_pre_visit_func(
    schema: typing.Mapping[str, typing.Any],
    namespace: str,
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
    parent_type: str = None,
) -> typing.Tuple[bool, typing.Union[typing.Mapping[str, typing.Any], str]]:
    schema_name = schema.get("name", None)
    namespace = schema.get("namespace", namespace)
    if schema_name in visited_schemas:
        namespace, _ = visited_schemas[schema_name]
        return True, namespace, (f'{namespace}.{schema_name}' if namespace else schema_name)
    if schema.get(SCHEMALESS_AVRO_SCHEMA):
        return False, namespace, get_schemaless_avro_schema(namespace)
    return False, namespace, schema


def _namespace_overriding_pre_visit_func(
    schema: typing.Mapping[str, typing.Any],
    namespace: str,
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
    parent_type: str = None,
) -> typing.Tuple[bool, typing.Union[typing.Mapping[str, typing.Any], str]]:
    schema_name = schema.get("name", None)
    namespace = schema.get("namespace", namespace)
    schema_type = schema.get("type", None)
    if schema_name and schema_type in ('record', 'enum'):
        namespace = f'{namespace}.{schema_name.lower()}'
        schema['namespace'] = namespace
    if schema_name in visited_schemas:
        namespace, _ = visited_schemas[schema_name]
        return True, namespace, (f'{namespace}.{schema_name}' if namespace else schema_name)
    if schema.get(SCHEMALESS_AVRO_SCHEMA):
        return False, namespace, get_schemaless_avro_schema(namespace)
    return False, namespace, schema


def _default_post_visit_func(
    schema: typing.Mapping[str, typing.Any],
    namespace: str,
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
    parent_type: str = None,
) -> typing.Mapping[str, typing.Any]:
    schema_name = schema.get("name", None)
    namespace = schema.get("namespace", namespace)
    schema_type = schema.get("type", None)
    if schema_name and schema_type in ('record', 'enum'):
        visited_schemas[schema_name] = (namespace, schema)
    return schema


def _faust_post_visit_func(
    schema: typing.Mapping[str, typing.Any],
    namespace: str,
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
    parent_type: str = None,
) -> typing.Mapping[str, typing.Any]:
    schema_name = schema.get("name", None)
    namespace = schema.get("namespace", namespace)
    schema_type = schema.get("type", None)
    faust_record = {
        "type": "record",
        "name": "__faust",
        "fields": [
            {
                "name": "ns",
                "type": "string"
            }
        ],
        "doc": "Faust meta data"
    }
    if namespace:
        faust_namespace = f'{namespace}.{"__faust"}'
        faust_record = {**faust_record, ** {
            "namespace": faust_namespace
        }}
    else:
        faust_namespace = None
    if "fields" in schema:
        if "__faust" in visited_schemas:
            additional_field = [{
                "name": "__faust",
                "type": faust_namespace + ".__faust"
            }]
        else:
            visited_schemas["__faust"] = (faust_namespace, faust_record)
            additional_field = [{
                "name": "__faust",
                "type": faust_record
            }]
        schema["fields"] += additional_field
    if schema_name and schema_type in ('record', 'enum'):
        visited_schemas[schema_name] = (namespace, schema)
    return schema


def _schema_visitor(
    schema: typing.Union[typing.Mapping[str, typing.Any], str],
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
    namespace: str = None,
    pre_visit_func: typing.Callable[
        [
            typing.Mapping[str, typing.Any],
            typing.Tuple[str, str],
            typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
            str,
        ],
        typing.Tuple[bool, typing.Union[typing.Mapping[str, typing.Any], str]]] = _default_pre_visit_func,
    post_visit_func: typing.Callable[
        [
            typing.Mapping[str, typing.Any],
            typing.Tuple[str, str],
            typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
            str,
        ],
        typing.Mapping[str, typing.Any]] = _default_post_visit_func,
    parent_type: str = None,
):
    if not isinstance(schema, typing.Mapping):
        return schema
    visited, namespace, schema = pre_visit_func(schema, namespace, visited_schemas, parent_type)
    if visited:
        return schema
    if schema["type"] in ('record'):
        for field in schema.get("fields", []):
            if isinstance(field["type"], typing.Mapping):
                field_schema = field["type"]
                if field_schema["type"] in ('record', 'enum'):
                    field["type"] = _schema_visitor(
                        field_schema, visited_schemas, namespace, pre_visit_func, post_visit_func, "fields")
                elif field_schema["type"] in ('array'):
                    if isinstance(field_schema["items"], typing.Mapping) and field_schema["items"]["type"] in ('record', 'enum'):
                        field_schema["items"] = _schema_visitor(
                            field_schema["items"], visited_schemas, namespace, pre_visit_func, post_visit_func, "array")
                    if isinstance(field_schema["items"], typing.Sequence) and not isinstance(field_schema["items"], str):
                        field_schema["items"] = [_schema_visitor(subschema, visited_schemas, namespace, pre_visit_func, post_visit_func, "array")
                                                 for subschema in field_schema["items"]]
                elif field_schema["type"] in ('map'):
                    if isinstance(field_schema["values"], typing.Mapping) and field_schema["values"]["type"] in ('record', 'enum'):
                        field_schema["values"] = _schema_visitor(
                            field_schema["values"], visited_schemas, namespace, pre_visit_func, post_visit_func, "map")
                    if isinstance(field_schema["values"], typing.Sequence) and not isinstance(field_schema["values"], str):
                        field_schema["values"] = [_schema_visitor(subschema, visited_schemas, namespace, pre_visit_func, post_visit_func, "map")
                                                  for subschema in field_schema["values"]]
            if isinstance(field["type"], typing.Sequence) and not isinstance(field["type"], str):
                field["type"] = [_schema_visitor(subschema, visited_schemas, namespace, pre_visit_func, post_visit_func, "fields")
                                 for subschema in field["type"]]
    elif schema["type"] in ('array'):
        if isinstance(schema["items"], typing.Mapping) and schema["items"]["type"] in ('record', 'enum'):
            schema["items"] = _schema_visitor(
                schema["items"], visited_schemas, namespace, pre_visit_func, post_visit_func, "array")
        if isinstance(schema["items"], typing.Sequence) and not isinstance(schema["items"], str):
            schema["items"] = [_schema_visitor(subschema, visited_schemas, namespace, pre_visit_func, post_visit_func, "array")
                               for subschema in schema["items"]]
    elif schema["type"] in ('map'):
        if isinstance(schema["values"], typing.Mapping) and schema["values"]["type"] in ('record', 'enum'):
            schema["values"] = _schema_visitor(
                schema["values"], visited_schemas, namespace, pre_visit_func, post_visit_func, "map")
        if isinstance(schema["values"], typing.Sequence) and not isinstance(schema["values"], str):
            schema["values"] = [_schema_visitor(subschema, visited_schemas, namespace, pre_visit_func, post_visit_func, "map")
                                for subschema in schema["values"]]
    schema = post_visit_func(schema, namespace, visited_schemas, parent_type)
    return schema
