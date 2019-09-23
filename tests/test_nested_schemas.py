import json
import typing

from dataclasses_avroschema.schema_generator import SchemaGenerator


def test_one_to_one_relationship(user_one_address_schema):
    """
    Test schema relationship one-to-one
    """
    class Address:
        "An Address"
        street: str
        street_number: int

    class Phone:
        "An Phonenumber"
        number: str

    class User:
        "An User with Address"
        name: str
        age: int
        address: Address
        phone: Phone

    schema = SchemaGenerator(User).avro_schema()
    assert schema == json.dumps(user_one_address_schema)


def test_one_to_many_relationship(user_many_address_schema):
    """
    Test schema relationship one-to-many
    """
    class Address:
        "An Address"
        street: str
        street_number: int

    class Phone:
        "An Phonenumber"
        number: str

    class User:
        "User with multiple Address"
        name: str
        age: int
        addresses: typing.List[Address]
        phones: typing.List[Phone]

    schema = SchemaGenerator(User).avro_schema()
    assert schema == json.dumps(user_many_address_schema)


def test_one_to_many_map_relationship(user_many_address_map_schema):
    """
    Test schema relationship one-to-many using a map
    """
    class Address:
        "An Address"
        street: str
        street_number: int

    class Phone:
        "An Phonenumber"
        number: str

    class User:
        "User with multiple Address"
        name: str
        age: int
        addresses: typing.Dict[str, Address]
        phones: typing.Dict[str, Phone]

    schema = SchemaGenerator(User).avro_schema()
    assert schema == json.dumps(user_many_address_map_schema)


# def test_recursive_one_to_one_relationship(user_self_refernece_schema):
#     """
#     Test self relationship one-to-one
#     """

#     class User:
#         "User with self reference as friend"
#         name: str
#         age: int
#         friend: typing.Type["User"]

#     schema = SchemaGenerator(User).avro_schema()

#     assert schema == json.dumps(user_self_refernece_schema)


def test_recursive_one_to_many_relationship(user_self_refernece_schema):
    """
    Test self relationship one-to-many
    """

    class User:
        "User with self reference as friends"
        name: str
        age: int
        friends: typing.List[typing.Type["User"]]

    schema = SchemaGenerator(User).avro_schema()

    assert schema == json.dumps(user_self_refernece_schema)
