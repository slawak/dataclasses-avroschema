import abc
import collections
import dataclasses
import datetime
import json
import typing
import uuid
import enum

from collections import OrderedDict

import inflect

from dataclasses_avroschema import schema_generator, types, utils

from .constants import (
    BOOLEAN,
    NULL,
    INT,
    FLOAT,
    DOUBLE,
    LONG,
    BYTES,
    STRING,
    ARRAY,
    ENUM,
    MAP,
    FIXED,
    DATE,
    TIME_MILLIS,
    TIMESTAMP_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MICROS,
    UUID,
    LOGICAL_DATE,
    LOGICAL_TIME,
    LOGICAL_DATETIME,
    LOGICAL_UUID,

    PYTHON_TYPE_TO_AVRO,
    PYTHON_INMUTABLE_TYPES,
    PYTHON_PRIMITIVE_CONTAINERS,
    PYTHON_LOGICAL_TYPES,
    PYTHON_PRIMITIVE_TYPES,
    PRIMITIVE_AND_LOGICAL_TYPES,

    PythonPrimitiveTypes,
)

from .schemaless_avro_codec import (
    SCHEMALESS_AVRO_SCHEMA,
    SCHEMALESS_AVRO_SCHEMA_NAME,
    get_schemaless_avro_schema,
)

p = inflect.engine()


@dataclasses.dataclass
class BaseField:
    avro_type: typing.ClassVar

    name: str
    type: typing.Any  # store the python primitive type
    default: typing.Any = dataclasses.MISSING
    metadata: typing.Dict = dataclasses.MISSING

    @staticmethod
    def _get_self_reference_type(a_type):
        internal_type = a_type.__args__[0]

        return internal_type.__forward_arg__

    @staticmethod
    def get_singular_name(name):
        singular = p.singular_noun(name)

        if singular:
            return singular
        return name

    def get_metadata(self) -> typing.List[typing.Tuple[str, str]]:
        meta_data_for_template = []
        try:
            metadata = dict(self.metadata)
            for name, value in metadata.items():
                meta_data_for_template.append((name, value))
        except (ValueError, TypeError):
            return meta_data_for_template
        return meta_data_for_template

    def render(self) -> OrderedDict:
        """
        Render the fields base on the avro field

        At least will have name and type.

        returns:
            OrderedDict(
                ("name", "a name"),
                ("type", "a type"),
                ("default", "default value")
            )

            The default key is optional.

            If self.type is:
                * list, the OrderedDict will contains the key items inside type
                * tuple, he OrderedDict will contains the key symbols inside type
                * dict, he OrderedDict will contains the key values inside type
        """
        if self.name:
            template = OrderedDict(
                [("name", self.name), ("type", self.get_avro_type())] + self.get_metadata()
            )
        else:
            template = OrderedDict(
                [("type", self.get_avro_type())] + self.get_metadata()
            )

        default = self.get_default_value()
        if default is not None:
            template["default"] = default

        return template

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return NULL

            if self.validate_default():
                return self.default

    def validate_default(self):
        msg = f"Invalid default type. Default should be {self.type}"
        assert isinstance(self.default, self.type), msg

        return True

    def to_json(self) -> str:
        return json.dumps(self.render(), indent=2)

    def to_dict(self) -> dict:
        return json.loads(self.to_json())

    @abc.abstractmethod
    def get_avro_type(self):
        ...  # pragma: no cover


class InmutableField(BaseField):
    def get_avro_type(self) -> PythonPrimitiveTypes:
        if self.default is not dataclasses.MISSING:
            if self.default is not None:
                return [self.avro_type, NULL]
            # means that default value is None
            return [NULL, self.avro_type]

        return self.avro_type


@dataclasses.dataclass
class StringField(InmutableField):
    avro_type: typing.ClassVar = STRING


@dataclasses.dataclass
class IntegerField(InmutableField):
    avro_type: typing.ClassVar = INT


@dataclasses.dataclass
class BooleanField(InmutableField):
    avro_type: typing.ClassVar = BOOLEAN


@dataclasses.dataclass
class FloatField(InmutableField):
    avro_type: typing.ClassVar = FLOAT


@dataclasses.dataclass
class DoubleField(InmutableField):
    avro_type: typing.ClassVar = DOUBLE


@dataclasses.dataclass
class BytesField(InmutableField):
    avro_type: typing.ClassVar = BYTES


@dataclasses.dataclass
class NoneField(InmutableField):
    avro_type: typing.ClassVar = NULL


@dataclasses.dataclass
class ContainerField(BaseField):
    def get_avro_type(self) -> PythonPrimitiveTypes:
        avro_type = self.avro_type
        if self.name:
            avro_type["name"] = self.get_singular_name(self.name)

        return avro_type


@dataclasses.dataclass
class TupleField(ContainerField):
    symbols: typing.Any = None
    default_factory: typing.Any = None

    def __post_init__(self):
        self.generate_symbols()

    @property
    def avro_type(self) -> typing.Dict:
        return {"type": ENUM, "symbols": self.symbols}

    def get_default_value(self):
        return

    def generate_symbols(self):
        self.symbols = list(self.default)


@dataclasses.dataclass
class EnumField(ContainerField):
    symbols: typing.Any = None
    default_factory: typing.Any = None

    def __post_init__(self):
        self.generate_symbols()

    @property
    def avro_type(self) -> typing.Dict:
        return {"type": ENUM, "symbols": self.symbols}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.validate_default():
                return self.default

    def generate_symbols(self):
        self.symbols = [member.value for member in list(self.type)]


@dataclasses.dataclass
class ListField(ContainerField):
    items_type: typing.Any = None
    default_factory: typing.Any = None

    def __post_init__(self):
        if self.type.__args__[0] == typing.Any:
            self.name = SCHEMALESS_AVRO_SCHEMA_NAME
        else:
            self.generate_items_type()

    @property
    def avro_type(self) -> typing.Dict:
        if self.type.__args__[0] == typing.Any:
            return get_schemaless_avro_schema()
        else:
            if self.items_type.get(SCHEMALESS_AVRO_SCHEMA, False):
                self.name = SCHEMALESS_AVRO_SCHEMA_NAME
                return get_schemaless_avro_schema()
            else:
                return {"type": ARRAY, "items": self.items_type}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return []
        elif self.default_factory not in (dataclasses.MISSING, None):
            # expecting a callable
            default = self.default_factory()
            assert isinstance(
                default, list
            ), f"List is required as default for field {self.name}"

            logical_classes = LOGICAL_TYPES_FIELDS_CLASSES.keys()

            return [
                LOGICAL_TYPES_FIELDS_CLASSES[type(item)].to_logical_type(item)
                if type(item) in logical_classes
                else item
                for item in default
            ]

    def generate_items_type(self):
        # because avro can have only one type, we take the first one
        items_type = self.type.__args__[0]

        if items_type in PRIMITIVE_AND_LOGICAL_TYPES:
            klass = PRIMITIVE_LOGICAL_TYPES_FIELDS_CLASSES[items_type]
            self.items_type = klass.avro_type
        elif utils.is_self_referenced(items_type):
            # Checking for a self reference. Maybe is a typing.ForwardRef
            self.items_type = self._get_self_reference_type(items_type)
        elif utils.is_union(items_type):
            self.items_type = UnionField.generate_union(
                items_type.__args__,
                default=self.default,
                default_factory=self.default_factory,
            )
        else:
            items_type_field = Field(name=None, native_type=items_type).render()
            items_type = items_type_field["type"]
            self.items_type = items_type


@dataclasses.dataclass
class DictField(ContainerField):
    default_factory: typing.Any = None
    values_type: typing.Any = None

    def __post_init__(self):
        if self.type.__args__[1] == typing.Any:
            self.name = SCHEMALESS_AVRO_SCHEMA_NAME
        else:
            self.generate_values_type()

    @property
    def avro_type(self) -> typing.Dict:
        if self.type.__args__[1] == typing.Any:
            return get_schemaless_avro_schema()
        else:
            return {"type": MAP, "values": self.values_type}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return {}
        elif self.default_factory not in (dataclasses.MISSING, None):
            # expeting a callable
            default = self.default_factory()
            assert isinstance(
                default, dict
            ), f"Dict is required as default for field {self.name}"

            logical_classes = LOGICAL_TYPES_FIELDS_CLASSES.keys()

            return {
                key: LOGICAL_TYPES_FIELDS_CLASSES[type(value)].to_logical_type(value)
                if type(value) in logical_classes
                else value
                for key, value in default.items()
            }

    def generate_values_type(self):
        """
        Process typing.Dict. Avro assumes that the key of a map is always a string,
        so we take the second argument to determine the value type
        """
        values_type = self.type.__args__[1]

        if values_type in PRIMITIVE_AND_LOGICAL_TYPES:
            klass = PRIMITIVE_LOGICAL_TYPES_FIELDS_CLASSES[values_type]
            self.values_type = klass.avro_type
        elif utils.is_self_referenced(values_type):
            # Checking for a self reference. Maybe is a typing.ForwardRef
            self.values_type = self._get_self_reference_type(values_type)
        else:
            values_type_field = Field(name=None, native_type=values_type).render()
            values_type = values_type_field["type"]
            self.values_type = values_type


@dataclasses.dataclass
class UnionField(BaseField):
    default_factory: typing.Any = dataclasses.MISSING

    def get_avro_type(self):
        elements = self.type.__args__

        return self.generate_union(
            elements, default=self.default, default_factory=self.default_factory
        )

    @staticmethod
    def generate_union(
        elements: typing.List,
        default: typing.Any = None,
        default_factory: typing.Callable = dataclasses.MISSING,
    ):
        """
        Generate union.

        Arguments:
            elements (typing.List): List of python types
            default (typing.Any): Default value
            default factory (typing.Calleable): Callable to get the default value for
                a list or dict type

        Returns:
            typing.List: List of avro types
        """
        unions = []
        for element in elements:
            if element in PRIMITIVE_AND_LOGICAL_TYPES:
                klass = PRIMITIVE_LOGICAL_TYPES_FIELDS_CLASSES[element]
                union_element = klass.avro_type
            else:
                union_element_field = Field(name=None, native_type=element).render()
                union_element = union_element_field['type']
            unions.append(union_element)

        if default is None and default_factory is dataclasses.MISSING:
            # move NULL to first position
            try:
                unions.remove(NULL)
            except ValueError:
                pass
            unions.insert(0, NULL)

        return unions

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return NULL
        elif self.default_factory not in (dataclasses.MISSING, None):
            # expeting a callable
            default = self.default_factory()
            assert isinstance(
                default, (dict, list)
            ), f"Dict or List is required as default for field {self.name}"

            return default


@dataclasses.dataclass
class FixedField(BaseField):
    def get_avro_type(self):
        avro_type = {
            "type": FIXED,
            "name": self.get_singular_name(self.name),
            "size": int(self.default.size),
        }

        if self.default.namespace is not None:
            avro_type["namespace"] = self.default.namespace

        if self.default.aliases is not None:
            avro_type["aliases"] = self.default.aliases

        return avro_type

    def get_default_value(self):
        return


@dataclasses.dataclass
class SelfReferenceField(BaseField):
    def get_avro_type(self):
        return self._get_self_reference_type(self.type)

    def get_default_value(self):
        return


class LogicalTypeField(BaseField):
    def get_avro_type(self):
        return self.avro_type


@dataclasses.dataclass
class DateField(LogicalTypeField):
    """
    The date logical type represents a date within the calendar,
    with no reference to a particular time zone or time of day.

    A date logical type annotates an Avro int, where the int stores
    the number of days from the unix epoch, 1 January 1970 (ISO calendar).
    """

    avro_type: typing.ClassVar = {"type": INT, "logicalType": DATE}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return NULL

            if self.validate_default():
                # Convert to datetime and get the amount of days
                return self.to_logical_type(self.default)

    @staticmethod
    def to_logical_type(date):
        """
        Convert to datetime and get the amount of days
        from the unix epoch, 1 January 1970 (ISO calendar)
        for a given date

        Arguments:
            date (datetime.date)

        Returns:
            int
        """
        date_time = datetime.datetime.combine(date, datetime.datetime.min.time())
        ts = (date_time - datetime.datetime(1970, 1, 1)).total_seconds()

        return int(ts / (3600 * 24))


@dataclasses.dataclass
class TimeField(LogicalTypeField):
    """
    The time-micros logical type represents a time of day,
    with no reference to a particular calendar,
    time zone or date, with a precision of one microsecond.

    A time-micros logical type annotates an Avro int,
    where the int stores the number of microseconds after midnight, 00:00:00.000000.
    """

    avro_type: typing.ClassVar = {"type": LONG, "logicalType": TIME_MICROS}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return NULL

            if self.validate_default():
                return self.to_logical_type(self.default)

    @staticmethod
    def to_logical_type(time):
        """
        Returns the number of microseconds after midnight, 00:00:00.000
        for a given time object

        Arguments:
            time (datetime.time)

        Returns:
            int
        """
        hour, minutes, seconds, microseconds = (
            time.hour,
            time.minute,
            time.second,
            time.microsecond,
        )

        return int(
            (((hour * 60 + minutes) * 60 + seconds) * 1000 * 1000) + (microseconds)
        )


@dataclasses.dataclass
class DatetimeField(LogicalTypeField):
    """
    The timestamp-micros logical type represents an instant on the global timeline,
    independent of a particular time zone or calendar, with a precision of one microsecond.

    A timestamp-micros logical type annotates an Avro long,
    where the long stores the number of microseconds from the unix epoch,
    1 January 1970 00:00:00.000 UTC.
    """

    avro_type: typing.ClassVar = {"type": LONG, "logicalType": TIMESTAMP_MICROS}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return NULL

            if self.validate_default():
                return self.to_logical_type(self.default)

    @staticmethod
    def to_logical_type(date_time):
        """
        Returns the number of microseconds from the unix epoch,
        1 January 1970 00:00:00.000 UTC for a given datetime

        Arguments:
            date_time (datetime.datetime)

        Returns:
            float
        """
        ts = (date_time - datetime.datetime(1970, 1, 1)).total_seconds()
        return ts * 10**6


@dataclasses.dataclass
class UUIDField(LogicalTypeField):
    avro_type: typing.ClassVar = {"type": STRING, "logicalType": UUID}

    def get_default_value(self):
        if self.default is not dataclasses.MISSING:
            if self.default is None:
                return NULL

            if self.validate_default():
                return self.to_logical_type(self.default)

    def validate_default(self):
        msg = f"Invalid default type. Default should be {str} or {uuid.UUID}"
        assert isinstance(self.default, (str, uuid.UUID)), msg

        return True

    @staticmethod
    def to_logical_type(uuid4):
        return str(uuid4)


@dataclasses.dataclass
class RecordField(BaseField):
    def get_avro_type(self):
        if self.type == typing.Any:
            return get_schemaless_avro_schema()
        else:
            return schema_generator.SchemaGenerator(self.type).avro_schema_to_python()


INMUTABLE_FIELDS_CLASSES = {
    bool: BooleanField,
    int: IntegerField,
    float: DoubleField,
    bytes: BytesField,
    str: StringField,
    type(None): NoneField,
}

CONTAINER_FIELDS_CLASSES = {
    tuple: TupleField,
    list: ListField,
    collections.abc.Sequence: ListField,
    collections.abc.MutableSequence: ListField,
    dict: DictField,
    collections.abc.Mapping: DictField,
    collections.abc.MutableMapping: DictField,
    typing.Union: UnionField,
}

LOGICAL_TYPES_FIELDS_CLASSES = {
    datetime.date: DateField,
    datetime.time: TimeField,
    datetime.datetime: DatetimeField,
    uuid.uuid4: UUIDField,
    uuid.UUID: UUIDField,
}

PRIMITIVE_LOGICAL_TYPES_FIELDS_CLASSES = {
    **INMUTABLE_FIELDS_CLASSES,
    **LOGICAL_TYPES_FIELDS_CLASSES,
    types.Fixed: FixedField,
}


FieldType = typing.Union[
    StringField,
    BooleanField,
    FloatField,
    BytesField,
    NoneField,
    TupleField,
    ListField,
    DictField,
    UnionField,
    FixedField,
    SelfReferenceField,
    LogicalTypeField,
    DateField,
    TimeField,
    DatetimeField,
    UUIDField,
    RecordField,
]


def field_factory(
    name: str,
    native_type: typing.Any,
    default: typing.Any = dataclasses.MISSING,
    default_factory: typing.Any = dataclasses.MISSING,
    metadata: typing.Dict = dataclasses.MISSING,
) -> FieldType:
    if native_type in PYTHON_INMUTABLE_TYPES:
        klass = INMUTABLE_FIELDS_CLASSES[native_type]
        return klass(name=name, type=native_type, default=default, metadata=metadata)
    elif utils.is_self_referenced(native_type):
        return SelfReferenceField(
            name=name, type=native_type, default=default, metadata=metadata
        )
    elif native_type is types.Fixed:
        return FixedField(
            name=name, type=native_type, default=default, metadata=metadata
        )
    elif isinstance(native_type, typing._GenericAlias):
        origin = native_type.__origin__

        if origin not in (
            tuple,
            list,
            dict,
            typing.Union,
            collections.abc.Sequence,
            collections.abc.MutableSequence,
            collections.abc.Mapping,
            collections.abc.MutableMapping,
        ):
            raise ValueError(
                f"""
                Invalid Type for field {name}. Accepted types are list, tuple, dict or typing.Union
                """
            )

        klass = CONTAINER_FIELDS_CLASSES[origin]
        return klass(
            name=name,
            type=native_type,
            default=default,
            default_factory=default_factory,
            metadata=metadata,
        )
    elif native_type in PYTHON_LOGICAL_TYPES:
        klass = LOGICAL_TYPES_FIELDS_CLASSES[native_type]
        return klass(name=name, type=native_type, default=default, metadata=metadata)
    elif isinstance(native_type, type) and issubclass(native_type, enum.Enum):
        return EnumField(
            name=name, type=native_type, default=default, metadata=metadata
        )
    else:
        return RecordField(
            name=name, type=native_type, default=default, metadata=metadata
        )


Field = field_factory
