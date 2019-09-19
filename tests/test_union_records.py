import json
import typing
from fastavro import parse_schema

from dataclasses_avroschema.schema_generator import SchemaGenerator


def test_one_to_one_relationship(user_union_record_schema, user_union_record_dataclass):
    """
    Test union
    """
    parse_schema(user_union_record_schema)
    schema = SchemaGenerator(user_union_record_dataclass).avro_schema()
    assert schema == json.dumps(user_union_record_schema)