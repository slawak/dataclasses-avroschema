import inspect
import json
import typing
import datetime
import dateutil.tz
import uuid
import dataclasses
import io
from fastavro._schema_py import parse_schema
from fastavro._write_py import schemaless_writer
from fastavro._read_py import schemaless_reader

try:
    import faust
    from dataclasses_avroschema.schema_generator import SchemaGenerator
    from dataclasses_avroschema.fields_faust import AvroFieldDescriptor, EnumFieldDescriptor
except ImportError:
    faust = None  # type: ignore


def test_faust_record(record_faust_schema):
    """
    Test schema with derived from faust.Record types
    """

    if not faust:
        return


    @dataclasses.dataclass
    class TestRecordChildBase(faust.Record, polymorphic_fields=True):
        "Child Record Base"
        id: str

    @dataclasses.dataclass
    class TestRecordChild1(TestRecordChildBase):
        "Child Record1"
        id1: str

    @dataclasses.dataclass
    class TestRecordChild2(TestRecordChildBase):
        "Child Record2"
        id2: str

    @dataclasses.dataclass
    class TestRecord(faust.Record, polymorphic_fields=True):
        "A Record"
        children: typing.List[TestRecordChildBase] = AvroFieldDescriptor(
            avro_type=typing.List[typing.Union[TestRecordChild1, TestRecordChild2]],
        )
        timestamp: datetime.datetime
        date: datetime.date
        time: datetime.time
        uuid: uuid.UUID
        tuple_field: str = EnumFieldDescriptor(symbols = ['YES','NO'])
        nullable_date: datetime.date = None

    schema = SchemaGenerator(TestRecord).avro_schema(flat=True, faust_field=True)
    parsed_schema = parse_schema(json.loads(schema))
    loaded_schema = json.dumps(record_faust_schema)
    assert schema == loaded_schema

    test_record = TestRecord(
        children = [TestRecordChild1(id = "testtest1", id1 = "test"),TestRecordChild2(id = "testtest2", id2 = "test2")],
        timestamp = datetime.datetime(2000,1,10,tzinfo=dateutil.tz.tzutc()),
        date = datetime.date(2000,1,11),
        time = datetime.time(12,1,1),
        uuid = uuid.UUID('12345678123456781234567812345678'),
        tuple_field="YES",
        nullable_date=None,
        )
    test_dict = test_record.to_representation()
    
    bio = io.BytesIO()
    schemaless_writer(bio, parsed_schema, test_dict)
    assert len(bio.getvalue()) > 1
    bio.seek(0)
    test_dict_deser = schemaless_reader(bio, parsed_schema)
    assert test_dict_deser == test_dict_deser
    test_record_deser = TestRecord.from_data(test_dict_deser)
    assert test_record == test_record_deser
    
