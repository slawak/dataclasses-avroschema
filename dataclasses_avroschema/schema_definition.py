import dataclasses
import inspect
import typing
from collections import OrderedDict

from dataclasses_avroschema import fields


@dataclasses.dataclass
class BaseSchemaDefinition:
    """
    Minimal Schema definition
    """
    type: str
    klass_or_instance: dataclasses.dataclass

    def get_rendered_fields(self):
        raise NotImplementedError

    def render(self):
        raise NotImplementedError

    def get_schema_name(self):
        if inspect.isclass(self.klass_or_instance):
            return self.klass_or_instance.__name__
        return self.klass_or_instance.__class__.__name__

    def generate_documentation(self):
        doc = self.klass_or_instance.__doc__

        if doc is not None:
            return doc.replace("\n", "")


@dataclasses.dataclass
class AvroSchemaDefinition(BaseSchemaDefinition):
    aliases: typing.List[str] = None
    namespace: str = None
    fields: typing.List["fields.Field"] = None
    include_schema_doc: bool = True

    def __post_init__(self):
        self.generate_extra_avro_attributes()
        self.fields = self.parse_dataclasses_fields()

    def parse_dataclasses_fields(self) -> typing.List["fields.Field"]:
        return [
            fields.Field(
                dataclass_field.name,
                dataclass_field.type,
                dataclass_field.default,
                dataclass_field.default_factory
            )
            for dataclass_field in dataclasses.fields(self.klass_or_instance)
        ]

    def get_rendered_fields(self) -> typing.List["fields.Field"]:
        return [
            field.render() for field in self.fields
        ]

    def generate_extra_avro_attributes(self) -> None:
        """
        Look for the method in the dataclass.

        After calling the method extra_avro_attributes a dict is expected:
            typing.Dict[str, typing.Any]
        """
        extra_avro_attributes_fn = getattr(self.klass_or_instance, "extra_avro_attributes", None)

        if extra_avro_attributes_fn:
            extra_avro_attributes = extra_avro_attributes_fn()
            assert isinstance(extra_avro_attributes, dict), "Dict must be returned type in extra_avro_attributes method"

            aliases = extra_avro_attributes.get("aliases", self.aliases)
            namespace = extra_avro_attributes.get("namespace", self.namespace)

            self.aliases = aliases
            self.namespace = namespace

    def render(self):
        schema = OrderedDict([
            ("type", self.type),
            ("name", self.get_schema_name()),
            ("fields", self.get_rendered_fields())
        ])

        if self.include_schema_doc:
            doc = self.generate_documentation()
            if doc is not None:
                schema["doc"] = doc

        if self.namespace is not None:
            schema["namespace"] = self.namespace

        if self.aliases is not None:
            schema["aliases"] = self.aliases

        return schema
