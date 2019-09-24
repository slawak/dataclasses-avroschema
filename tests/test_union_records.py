import json
import typing
from fastavro import parse_schema

from dataclasses_avroschema.schema_generator import SchemaGenerator


def test_union_record(user_union_record_schema, user_union_record_dataclass):
    """
    Test union
    """
    parse_schema(user_union_record_schema)
    schema = SchemaGenerator(user_union_record_dataclass).avro_schema()
    loaded_schema = json.dumps(user_union_record_schema)
    assert schema == loaded_schema

def test_complex_union_record(user_complex_union_record_dataclass):
    """
    Test union
    """
    schema = SchemaGenerator(user_complex_union_record_dataclass).avro_schema()
    parse_schema(json.loads(schema))


def test_complex_union_map_record(user_complex_union_record_map_dataclass):
    """
    Test union
    """
    schema = SchemaGenerator(user_complex_union_record_map_dataclass).avro_schema()
    parse_schema(json.loads(schema))
