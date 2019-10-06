import json
import dataclasses
import typing
import inflect
import datetime
import uuid

from collections import OrderedDict

from dataclasses_avroschema import schema_generator

try:
    import faust
    from dataclasses_avroschema import fields_faust
except ImportError:
    faust = None  # type: ignore

p = inflect.engine()

BOOLEAN = "boolean"
NULL = "null"
INT = "int"
FLOAT = "double"
BYTES = "bytes"
STRING = "string"
ARRAY = "array"
ENUM = "enum"
MAP = "map"
LONG = "long"
TIMESTAMP_MICROS = "timestamp-micros"
TIME_MICROS = "time-micros"
DATE = "date"
UUID = "uuid"


PYTHON_TYPE_TO_AVRO = {
    bool: BOOLEAN,
    None: NULL,
    int: INT,
    float: FLOAT,
    bytes: BYTES,
    str: STRING,
    list: {"type": ARRAY},
    tuple: {"type": ENUM},
    dict: {"type": MAP},
    datetime.time: {"type": LONG, "logicalType": TIME_MICROS},
    datetime.datetime: {"type": LONG, "logicalType": TIMESTAMP_MICROS},
    datetime.date: {"type": INT, "logicalType": DATE},
    uuid.UUID: {"type": STRING, "logicalType": UUID},
}

# excluding tuple because is a container
PYTHON_PRIMITIVE_TYPES = (str, int, bool, float, bytes)

PYTHON_LOGICAL_TYPES = (datetime.date, datetime.time, datetime.datetime, uuid.UUID)

PYTHON_INMUTABLE_TYPES = PYTHON_PRIMITIVE_TYPES + PYTHON_LOGICAL_TYPES

PYTHON_PRIMITIVE_CONTAINERS = (list, tuple, dict)

PYTHON_PRIMITIVE_TYPES = PYTHON_INMUTABLE_TYPES + PYTHON_PRIMITIVE_CONTAINERS

PythonPrimitiveTypes = typing.Union[str, int, bool, float, list, tuple, dict]

@dataclasses.dataclass
class Field:
    name: str
    type: typing.Any  # store the python type
    default: typing.Any = dataclasses.MISSING
    default_factory: typing.Any = None
    namespace: str = None

    # for avro array field
    items_type: typing.Any = None

    # for avro enum field
    symbols: typing.Any = None

    # for avro map field
    values_type: typing.Any = None

    # avro type storing
    avro_type: typing.Any = None

    def __post_init__(self):
        # special annotations for faust records to handle union types
        if faust and isinstance(self.default, fields_faust.AvroFieldDescriptor):
            self.type = self.default.avro_type
        # special annotations for faust recors to handle enums
        if faust and isinstance(self.default, fields_faust.EnumFieldDescriptor):
            self.type = typing.Tuple[str]
        if isinstance(self.type, typing._GenericAlias):
           # means that could be a list, tuple or dict
            origin = self.type.__origin__
            processor = self.get_processor(origin)
            processor()

            self.type = origin

    def get_processor(self, origin):
        if origin is list:
            return self._process_list_type
        elif origin is dict:
            return self._process_dict_type
        elif origin is tuple:
            return self._process_tuple_type
        elif origin is typing.Union:
            return self._process_union_type
        elif origin is type:
            return self._process_self_reference_type_single
        else:
            # we do not accept any other typing._GenericAlias like a set
            # we should raise an exception
            raise ValueError(
                f"Invalid Type for field {self.name}. Accepted types are list, tuple or dict")

    def _process_list_type(self):
        # because avro can have only one type, we take the first one
        items_type = self.type.__args__[0]
        
        if items_type in PYTHON_PRIMITIVE_TYPES:
            self.items_type = PYTHON_TYPE_TO_AVRO[items_type]
        elif hasattr(items_type, '__origin__') and items_type.__origin__ is typing.Union:
            self.items_type = list(self._process_union_type_list(items_type.__args__))
        elif isinstance(items_type, typing._GenericAlias):
            # Checking for a self reference. Maybe is a typing.ForwardRef
            self.items_type = self._process_self_reference_type(items_type)
        else:
            # means is a custom type
            self.items_type = schema_generator.SchemaGenerator(
                items_type).avro_schema_to_python()

    def _process_dict_type(self):
        # because avro can have only one type, we take the first one
        values_type = self.type.__args__[1]

        if values_type in PYTHON_PRIMITIVE_TYPES:
            self.values_type = PYTHON_TYPE_TO_AVRO[values_type]
        elif hasattr(values_type, '__origin__') and values_type.__origin__ is typing.Union:
            self.values_type = list(self._process_union_type_list(values_type.__args__))
        elif isinstance(values_type, typing._GenericAlias):
            # Checking for a self reference. Maybe is a typing.ForwardRef
            self.values_type = self._process_self_reference_type(values_type)
        else:
            self.values_type = schema_generator.SchemaGenerator(
                values_type).avro_schema_to_python()

    def _process_tuple_type(self):
        if faust and isinstance(self.default, fields_faust.EnumFieldDescriptor):
            self.symbols = list(self.default.symbols)
        else:    
            self.symbols = list(self.default)

    def _process_self_reference_type_single(self):
        self.values_type = self._process_self_reference_type(self.type)

    def _process_self_reference_type(self, items_type):
        internal_type = items_type.__args__[0]

        assert isinstance(internal_type, typing.ForwardRef), "Expecting a self reference"
        return internal_type.__forward_arg__

    def _process_union_type(self):
        self.values_type = list(self._process_union_type_list(self.type.__args__))

    def _process_union_type_list(self, values_types):
        for values_type in values_types:
            if values_type in PYTHON_PRIMITIVE_TYPES:
                yield PYTHON_TYPE_TO_AVRO[values_type]
            elif values_type is type(None):
                yield NULL
            elif isinstance(values_type, typing._GenericAlias) \
                    and values_type.__origin__ is type \
                    and isinstance(values_type.__args__[0], typing.ForwardRef):
                yield self._process_self_reference_type(values_type)
            else:
                yield schema_generator.SchemaGenerator(
                    values_type).avro_schema_to_python()

    @staticmethod
    def get_singular_name(name):
        singular = p.singular_noun(name)

        if singular:
            return singular
        return name

    def get_avro_type(self) -> PythonPrimitiveTypes:
        avro_type = PYTHON_TYPE_TO_AVRO.get(self.type)
        # copy record template
        if isinstance(avro_type, dict):
            avro_type = dict(avro_type)

        if self.type in PYTHON_INMUTABLE_TYPES:
            if self.default is not dataclasses.MISSING and self.type is not tuple \
                    and (not faust or faust and not isinstance(self.default, faust.models.FieldDescriptor) \
                    or isinstance(self.default, faust.models.FieldDescriptor) and not self.default.required):
                if self.default is not None:
                    return [avro_type, NULL]
                # means that default value is None
                return [NULL, avro_type]

            return avro_type
        elif self.type in PYTHON_PRIMITIVE_CONTAINERS:
            if self.items_type:
                avro_type["items"] = self.items_type
            elif self.values_type:
                avro_type["values"] = self.values_type
            elif self.symbols:
                avro_type["symbols"] = self.symbols
                if self.namespace:
                    avro_type["namespace"] = self.namespace

            avro_type["name"] = self.get_singular_name(self.name)
            return avro_type
        elif self.type is typing.Union:
            return self.values_type
        elif self.type is type:
            return self.values_type
        else:
            # we need to see what to to when is a custom type
            # is a record schema
            return schema_generator.SchemaGenerator(self.type).avro_schema_to_python()

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.type in PYTHON_INMUTABLE_TYPES:
                if faust and isinstance(self.default, faust.models.FieldDescriptor):
                    if self.default.required:
                        return None
                    if self.default.default is None:
                        return NULL
                    return self.default.default
                if self.default is None:
                    return NULL
                return self.default
            elif self.type is list:
                if self.default is None:
                    return []
            elif self.type is dict:
                if self.default is None:
                    return {}
        elif self.default_factory not in (dataclasses.MISSING, None):
            if self.type is list:
                # expeting a callable
                default = self.default_factory()
                assert isinstance(
                    default, list), f"List is required as default for field {self.name}"

                return default
            elif self.type is dict:
                # expeting a callable
                default = self.default_factory()
                assert isinstance(
                    default, dict), f"Dict is required as default for field {self.name}"

                return default

    def render(self) -> OrderedDict:
        """
        Render the fields base on the avro field

        At least will have name and type.

        returns:
            OrderedDict(
                ("name", "a name"),
                ("type", "a type")
            )

            The default key is optional.

            If self.type is:
                * list, the OrderedDict will contains the key items inside type
                * tuple, he OrderedDict will contains the key symbols inside type
                * dict, he OrderedDict will contains the key values inside type
        """
        template = OrderedDict([
            ("name", self.name),
            ("type", self.get_avro_type()),
        ])

        default = self.get_default_value()
        if default is not None:
            template["default"] = default

        return template

    def to_json(self) -> str:
        return json.dumps(self.render())

    def to_dict(self) -> dict:
        return json.loads(self.to_json())
