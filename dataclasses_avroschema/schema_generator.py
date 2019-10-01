import json
import dataclasses
import typing
import copy
import collections

from dataclasses_avroschema.schema_definition import AvroSchemaDefinition


class SchemaGenerator:

    def __init__(self, klass_or_instance, include_schema_doc: bool = True) -> None:
        self.dataclass = self.generate_dataclass(klass_or_instance)
        self.include_schema_doc = include_schema_doc
        self.schema_definition = None

    @staticmethod
    def generate_dataclass(klass_or_instance):
        if dataclasses.is_dataclass(klass_or_instance):
            return klass_or_instance
        return dataclasses.dataclass(klass_or_instance)

    def generate_schema(self, schema_type: str = "avro"):
        if self.schema_definition is not None:
            return self.schema_definition

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
            "record",
            self.dataclass,
            include_schema_doc=self.include_schema_doc
        )

    def avro_schema(self, flat:bool = False, faust_field:bool = False) -> str:
        schema = self.generate_schema(schema_type="avro")
        if flat:
            # a simple addition of a __faust field to all records to contain faust model metadata
            if faust_field:
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
                namespace = schema.get("namespace")
                if namespace:
                    faust_record = {**faust_record, ** {
                        "namespace" : namespace
                    }}
                    namespace += "." 
                else:
                    namespace = ""
                add_fields = [{
                    "name": "__faust",
                    "type": namespace + "__faust"
                }]
            else:
                add_fields = None
            schemas = [schema]
            flat_schemas = collections.OrderedDict()
            schemas_to_process = collections.OrderedDict()
            _flatten_schemas(schemas, flat_schemas, None, schemas_to_process)
            schemas = list(flat_schemas.values())
            if faust_field:
                for record in schemas:
                    if "fields" in record:
                        record["fields"] += add_fields
                schemas = [faust_record] + schemas
            return json.dumps(schemas)
        else:
            return json.dumps(schema)

    def avro_schema_to_python(self, flat:bool = False, faust_field:bool = False) -> typing.Dict[str, typing.Any]:
        return json.loads(self.avro_schema(flat, faust_field))

    @property
    def get_fields(self) -> typing.List["Field"]:
        if self.schema_definition is None:
            self.generate_schema()

        return self.schema_definition.fields

# following functions are a very crude way to traverse a list of schemas 
# and flatten them by replacing nested schemas with references
# this resulting list of schemas can then be used with fastavro 
# without the issue of redifinition of a schema
def _process_schema_single(
    schema: typing.Dict[str, typing.Any], 
    flat_schemas: typing.Dict[typing.Tuple[str], typing.Any], 
    namespace: str = None, 
    schemas_to_process: typing.Dict[typing.Tuple[str], typing.Any] = {},
    ):
    schema_name = schema["name"]
    schema_ns = schema.get("namespace", namespace)
    schema_key = (schema_ns, schema_name)
    if schema_key in flat_schemas:
        #maybe check equality
        return schema_name
    if not schema_key in schemas_to_process:
        schemas_to_process[schema_key] = schema
    else:
        #maybe check equality
        pass
    _flatten_schema(
            schema, 
            flat_schemas, 
            namespace, 
            schemas_to_process)
    return schema_name

def _process_schema(
    idx: int,
    schemas: typing.List[typing.Dict[str, typing.Any]], 
    flat_schemas: typing.Dict[typing.Tuple[str], typing.Any], 
    namespace: str = None, 
    schemas_to_process: typing.Dict[typing.Tuple[str], typing.Any] = {},
    ):
    schemas[idx] = _process_schema_single(
        schemas[idx], 
        flat_schemas, 
        namespace, 
        schemas_to_process)

def _flatten_schema(
    schema: typing.Dict[str, typing.Any], 
    flat_schemas: typing.Dict[typing.Tuple[str], typing.Any], 
    namespace: str = None, 
    schemas_to_process: typing.Dict[typing.Tuple[str], typing.Any] = {}
    ):
    schema_name = schema["name"]
    namespace = schema.get("namespace", namespace)
    schema_key = (namespace, schema_name)
    if schema_key in flat_schemas:
        return True
    for field in schema.get("fields", []):
        if isinstance(field["type"], dict):
            field_schema = field["type"]
            if field_schema["type"] in ('record', 'enum'):
                field["type"] = _process_schema_single(field_schema, 
                    flat_schemas, namespace, schemas_to_process)
            elif field_schema["type"] in ('array'):
                if isinstance(field_schema["items"], dict) and field_schema["items"]["type"] in ('record', 'enum'):
                    field_schema["items"] = _process_schema_single(
                                        field_schema["items"], 
                                        flat_schemas, 
                                        namespace, 
                                        schemas_to_process)
                if isinstance(field_schema["items"], list):
                    _flatten_schemas(field_schema["items"], 
                        flat_schemas, namespace, schemas_to_process)
            elif field_schema["type"] in ('map'):
                if isinstance(field_schema["values"], dict) and field_schema["values"]["type"] in ('record', 'enum'):
                    field_schema["values"] = _process_schema_single(
                                        field_schema["values"], 
                                        flat_schemas, 
                                        namespace, 
                                        schemas_to_process)
                if isinstance(field_schema["values"], list):
                    _flatten_schemas(field_schema["values"], 
                        flat_schemas, namespace, schemas_to_process)
        if isinstance(field["type"], list):
            _flatten_schemas(field["type"], 
                flat_schemas, namespace, schemas_to_process)
    flat_schemas[schema_key] = schema
    if schema_key in schemas_to_process:
        del schemas_to_process[schema_key]

def _flatten_schemas(
    schemas: typing.List[typing.Dict[str, typing.Any]], 
    flat_schemas: typing.Dict[typing.Tuple[str], typing.Any], 
    namespace: str = None, 
    schemas_to_process: typing.Dict[typing.Tuple[str], typing.Any] = {}
    ):
    for idx, schema in enumerate(copy.copy(schemas)):
        if isinstance(schema, dict):
            if schema["type"] in ('record', 'enum'):
                _process_schema(idx, schemas, 
                    flat_schemas, namespace, schemas_to_process)