import json
import typing
import datetime
import uuid
from fastavro import parse_schema

from dataclasses_avroschema.schema_generator import SchemaGenerator


def test_logical_types_relationship(record_logical_types_schema):
    """
    Test schema with logical types
    """
    class TestRecord:
        "A Record"
        timestamp: datetime.datetime
        date: datetime.date
        time: datetime.time
        uuid: uuid.UUID

    schema = SchemaGenerator(TestRecord).avro_schema()
    parse_schema(json.loads(schema))
    loaded_schema = json.dumps(record_logical_types_schema)
    assert schema == loaded_schema
