import os
import json
import dataclasses
import typing
import pytest


SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "schemas"
)

AVRO_SCHEMAS_DIR = os.path.join(SCHEMA_DIR, "avro")
JSON_SCHEMAS_DIR = os.path.join(SCHEMA_DIR, "json")


def load(fp):
    with open(fp, mode="r") as f:
        return f.read()


@pytest.fixture
def user_dataclass():
    @dataclasses.dataclass(repr=False)
    class User:
        name: str
        age: int
        has_pets: bool
        money: float
        encoded: bytes

    return User


@pytest.fixture
def user_v2_dataclass():
    @dataclasses.dataclass(repr=False)
    class UserV2:
        "A User V2"
        name: str
        age: int

    return UserV2


@pytest.fixture
def user_extra_avro_atributes_dataclass():
    @dataclasses.dataclass(repr=False)
    class UserAliasesNamespace:
        name: str
        age: int

        @staticmethod
        def extra_avro_attributes() -> typing.Dict[str, typing.Any]:
            return {
                "namespace": "test.com.ar/user/v1",
                "aliases": ["User", "My favorite User"]
            }

    return UserAliasesNamespace


@pytest.fixture
def user_advance_dataclass():
    class UserAdvance:
        name: str
        age: int
        pets: typing.List[str]
        accounts: typing.Dict[str, int]
        has_car: bool = False
        favorite_colors: typing.Tuple[str] = ("BLUE", "YELLOW", "GREEN")
        country: str = "Argentina"
        address: str = None

    return UserAdvance


@pytest.fixture
def user_advance_with_defaults_dataclass():
    class UserAdvance:
        name: str
        age: int
        pets: typing.List[str] = dataclasses.field(default_factory=lambda: ['dog', 'cat'])
        accounts: typing.Dict[str, int] = dataclasses.field(default_factory=lambda: {"key": 1})
        has_car: bool = False
        favorite_colors: typing.Tuple[str] = ("BLUE", "YELLOW", "GREEN")
        country: str = "Argentina"
        address: str = None

    return UserAdvance

@pytest.fixture
def user_union_record_dataclass():
    @dataclasses.dataclass(repr=False)
    class Phone:
        "Phone"
        number: str
    @dataclasses.dataclass(repr=False)
    class Email:
        "Email"
        mail: str

    @dataclasses.dataclass(repr=False)
    class UserUnionRecord:
        "UserUnionRecord"
        name: str
        age: int
        has_pets: bool
        money: float
        encoded: bytes
        contact: typing.Union[Phone, Email]

    return UserUnionRecord

@pytest.fixture
def user_complex_union_record_dataclass():
    @dataclasses.dataclass(repr=False)
    class Phone:
        "Phone"
        number: typing.Union[str,int]
    @dataclasses.dataclass(repr=False)
    class Email:
        "Email"
        mail: typing.List[str]

    @dataclasses.dataclass(repr=False)
    class UserUnionRecord:
        "UserUnionRecord"
        name: str
        age: int
        has_pets: bool
        money: float
        encoded: bytes
        contact: typing.List[typing.Union[Phone, Email]]

    return UserUnionRecord

@pytest.fixture
def user_complex_union_record_map_dataclass():
    @dataclasses.dataclass(repr=False)
    class Phone:
        "Phone"
        number: typing.Union[str,int]
    @dataclasses.dataclass(repr=False)
    class Email:
        "Email"
        mail: typing.List[str]

    @dataclasses.dataclass(repr=False)
    class UserUnionRecord:
        "UserUnionRecord"
        name: str
        age: int
        has_pets: bool
        money: float
        encoded: bytes
        contact: typing.Dict[str, typing.Union[Phone, Email]]

    return UserUnionRecord


def load_json(file_name):
    schema_path = os.path.join(AVRO_SCHEMAS_DIR, file_name)
    schema_string = load(schema_path)
    return json.loads(schema_string)


@pytest.fixture
def user_avro_json():
    return load_json("user.avsc")


@pytest.fixture
def user_v2_avro_json():
    return load_json("user_v2.avsc")


@pytest.fixture
def user_advance_avro_json():
    return load_json("user_advance.avsc")


@pytest.fixture
def user_advance_with_defaults_avro_json():
    return load_json("user_advance_with_defaults.avsc")


@pytest.fixture
def user_extra_avro_attributes():
    return load_json("user_extra_avro_attributes.avsc")


@pytest.fixture
def user_one_address_schema():
    return load_json("user_one_address.avsc")


@pytest.fixture
def user_many_address_schema():
    return load_json("user_many_address.avsc")


@pytest.fixture
def user_many_address_map_schema():
    return load_json("user_many_address_map.avsc")

@pytest.fixture
def user_self_referenece_schema():
    return load_json("user_self_reference_one_to_one.avsc")

@pytest.fixture
def user_self_referenece_union_schema():
    return load_json("user_self_reference_one_to_one_union.avsc")

@pytest.fixture
def user_self_referenece_list_schema():
    return load_json("user_self_reference_one_to_many.avsc")

@pytest.fixture
def user_self_referenece_map_schema():
    return load_json("user_self_reference_one_to_many_map.avsc")

@pytest.fixture
def user_union_record_schema():
    return load_json("user_union_record.avsc")
