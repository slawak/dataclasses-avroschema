import datetime
import uuid
import typing


from dataclasses_avroschema import types

BOOLEAN = "boolean"
NULL = "null"
INT = "int"
FLOAT = "float"
DOUBLE = "double"
LONG = "long"
BYTES = "bytes"
STRING = "string"
ARRAY = "array"
ENUM = "enum"
MAP = "map"
FIXED = "fixed"
DATE = "date"
TIME_MILLIS = "time-millis"
TIMESTAMP_MILLIS = "timestamp-millis"
TIME_MICROS = "time-micros"
TIMESTAMP_MICROS = "timestamp-micros"
UUID = "uuid"
LOGICAL_DATE = {"type": INT, "logicalType": DATE}
LOGICAL_TIME = {"type": INT, "logicalType": TIME_MICROS}
LOGICAL_DATETIME = {"type": LONG, "logicalType": TIMESTAMP_MICROS}
LOGICAL_UUID = {"type": STRING, "logicalType": UUID}

PYTHON_TYPE_TO_AVRO = {
    bool: BOOLEAN,
    type(None): NULL,
    int: INT,
    float: DOUBLE,
    bytes: BYTES,
    str: STRING,
    list: {"type": ARRAY},
    tuple: {"type": ENUM},
    dict: {"type": MAP},
    types.Fixed: {"type": FIXED},
    datetime.date: {"type": INT, "logicalType": DATE},
    datetime.time: {"type": LONG, "logicalType": TIME_MICROS},
    datetime.datetime: {"type": LONG, "logicalType": TIMESTAMP_MICROS},
    uuid.uuid4: {"type": STRING, "logicalType": UUID},
}

# excluding tuple because is a container
PYTHON_INMUTABLE_TYPES = (str, int, bool, float, bytes, type(None))

PYTHON_PRIMITIVE_CONTAINERS = (list, tuple, dict)

PYTHON_LOGICAL_TYPES = (datetime.date, datetime.time, datetime.datetime, uuid.uuid4)

PYTHON_PRIMITIVE_TYPES = PYTHON_INMUTABLE_TYPES + PYTHON_PRIMITIVE_CONTAINERS

PRIMITIVE_AND_LOGICAL_TYPES = PYTHON_INMUTABLE_TYPES + PYTHON_LOGICAL_TYPES

PythonPrimitiveTypes = typing.Union[str, int, bool, float, list, tuple, dict]
