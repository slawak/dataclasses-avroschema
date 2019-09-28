import json
import typing
import datetime
import uuid
from fastavro import parse_schema

try:
    import faust
except ImportError:
    faust = None  # type: ignore


from dataclasses_avroschema.schema_generator import SchemaGenerator


def test_faust_record_relationship(record_faust_schema):
    """
    Test schema with derived from faust.Record types
    """

    if not faust:
        return

    class TestRecord(faust.Record):
        "A Record"
        timestamp: datetime.datetime
        date: datetime.date
        time: datetime.time
        uuid: uuid.UUID
        tuple_field: typing.Tuple = ('YES','NO')
        nullable_date: datetime.date = None

    schema = SchemaGenerator(TestRecord).avro_schema()
    parse_schema(json.loads(schema))
    loaded_schema = json.dumps(record_faust_schema)
    assert schema == loaded_schema
