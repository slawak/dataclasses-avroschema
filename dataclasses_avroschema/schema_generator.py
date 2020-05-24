import dataclasses
import json
import typing
import collections
import copy

from dataclasses_avroschema.schema_definition import AvroSchemaDefinition
from dataclasses_avroschema.schemaless_avro_codec import get_schemaless_pathes


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

    def avro_schema(self, canonical: bool = False, faust_field: bool = False) -> str:
        schema = copy.deepcopy(self.generate_schema(schema_type="avro"))
        if canonical:
            if faust_field:
                # a simple addition of a __faust field to all records to contain faust model metadata
                schema = _schema_visitor(schema,
                                         previsit_func=_default_previsit_func,
                                         postvisit_func=_faust_postvisit_func,
                                         )
            else:
                schema = _schema_visitor(schema,
                                         previsit_func=_default_previsit_func,
                                         postvisit_func=_default_postvisit_func,
                                         )
        return json.dumps(schema)

    def avro_schema_to_python(self, canonical: bool = False, faust_field: bool = False) -> typing.Mapping[str, typing.Any]:
        return json.loads(self.avro_schema(canonical=canonical, faust_field=faust_field))


def _default_previsit_func(
    schema: typing.Mapping[str, typing.Any],
    schema_key: typing.Tuple[str, str],
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
) -> typing.Tuple[bool, typing.Union[typing.Mapping[str, typing.Any], str]]:
    _, schema_name = schema_key
    if schema_key in visited_schemas:
        return True, schema_name
    return False, schema


def _default_postvisit_func(
    schema: typing.Mapping[str, typing.Any],
    schema_key: typing.Tuple[str, str],
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
) -> typing.Mapping[str, typing.Any]:
    _, schema_name = schema_key
    schema_type = schema.get("type", None)
    if schema_name and schema_type in ('record', 'enum'):
        visited_schemas[schema_key] = schema
    return schema


def _faust_postvisit_func(
    schema: typing.Mapping[str, typing.Any],
    schema_key: typing.Tuple[str, str],
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]],
) -> typing.Mapping[str, typing.Any]:
    namespace, schema_name = schema_key
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
        faust_namespace = namespace
        faust_record = {**faust_record, ** {
            "namespace": faust_namespace
        }}
        faust_namespace += "."
    else:
        faust_namespace = ""
    if "fields" in schema:
        faust_schema_key = (faust_namespace, "__faust")
        if faust_schema_key in visited_schemas:
            additional_field = [{
                "name": "__faust",
                "type": faust_namespace + "__faust"
            }]
        else:
            visited_schemas[faust_schema_key] = faust_record
            additional_field = [{
                "name": "__faust",
                "type": faust_record
            }]
        schema["fields"] += additional_field
    if schema_name and schema_type in ('record', 'enum'):
        visited_schemas[schema_key] = schema
    return schema


def _schema_visitor(
    schema: typing.Union[typing.Mapping[str, typing.Any], str],
    previsit_func: typing.Callable[
        [
            typing.Mapping[str, typing.Any],
            typing.Tuple[str, str],
            typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]]
        ],
        typing.Tuple[bool, typing.Union[typing.Mapping[str, typing.Any], str]]] = _default_previsit_func,
    namespace: str = None,
    postvisit_func: typing.Callable[
        [
            typing.Mapping[str, typing.Any],
            typing.Tuple[str, str],
            typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]]
        ],
        typing.Mapping[str, typing.Any]] = _default_postvisit_func,
    visited_schemas: typing.Mapping[typing.Tuple[str, str], typing.Mapping[str, typing.Any]] = {},

):
    if not isinstance(schema, typing.Mapping):
        return schema
    schema_name = schema.get("name", None)
    namespace = schema.get("namespace", namespace)
    schema_key = (namespace, schema_name)
    visited, schema = previsit_func(schema, schema_key, visited_schemas)
    if visited:
        return schema
    if schema["type"] in ('record'):
        for field in schema.get("fields", []):
            if isinstance(field["type"], typing.Mapping):
                field_schema = field["type"]
                if field_schema["type"] in ('record', 'enum'):
                    field["type"] = _schema_visitor(
                        field_schema, previsit_func, namespace, postvisit_func, visited_schemas)
                elif field_schema["type"] in ('array'):
                    if isinstance(field_schema["items"], typing.Mapping) and field_schema["items"]["type"] in ('record', 'enum'):
                        field_schema["items"] = _schema_visitor(
                            field_schema["items"], previsit_func, namespace, postvisit_func, visited_schemas)
                    if isinstance(field_schema["items"], typing.Sequence) and not isinstance(field_schema["items"], str):
                        field_schema["items"] = [_schema_visitor(subschema, previsit_func, namespace, postvisit_func, visited_schemas)
                                                 for subschema in field_schema["items"]]
                elif field_schema["type"] in ('map'):
                    if isinstance(field_schema["values"], typing.Mapping) and field_schema["values"]["type"] in ('record', 'enum'):
                        field_schema["values"] = _schema_visitor(
                            field_schema["values"], previsit_func, namespace, postvisit_func, visited_schemas)
                    if isinstance(field_schema["values"], typing.Sequence) and not isinstance(field_schema["values"], str):
                        field_schema["values"] = [_schema_visitor(subschema, previsit_func, namespace, postvisit_func, visited_schemas)
                                                  for subschema in field_schema["values"]]
            if isinstance(field["type"], typing.Sequence) and not isinstance(field["type"], str):
                field["type"] = [_schema_visitor(subschema, previsit_func, namespace, postvisit_func, visited_schemas)
                                 for subschema in field["type"]]
    elif schema["type"] in ('array'):
        if isinstance(schema["items"], typing.Mapping) and schema["items"]["type"] in ('record', 'enum'):
            schema["items"] = _schema_visitor(
                schema["items"], previsit_func, namespace, postvisit_func, visited_schemas)
        if isinstance(schema["items"], typing.Sequence) and not isinstance(schema["items"], str):
            schema["items"] = [_schema_visitor(subschema, previsit_func, namespace, postvisit_func, visited_schemas)
                               for subschema in schema["items"]]
    elif schema["type"] in ('map'):
        if isinstance(schema["values"], typing.Mapping) and schema["values"]["type"] in ('record', 'enum'):
            schema["values"] = _schema_visitor(
                schema["values"], previsit_func, namespace, postvisit_func, visited_schemas)
        if isinstance(schema["values"], typing.Sequence) and not isinstance(schema["values"], str):
            schema["values"] = [_schema_visitor(subschema, previsit_func, namespace, postvisit_func, visited_schemas)
                                for subschema in schema["values"]]
    schema = postvisit_func(schema, schema_key, visited_schemas)
    return schema
