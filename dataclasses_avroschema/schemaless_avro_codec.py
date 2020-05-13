import typing
import collections

from collections import OrderedDict
from collections.abc import Iterable, Mapping, MutableMapping, MutableSequence

from .constants import (
    BOOLEAN,
    NULL,
    INT,
    FLOAT,
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

SCHEMALESS_AVRO_SCHEMA = '__schemaless_avro_schema__'
SCHEMALESS_AVRO_SCHEMA_NAME = 'schemaless_object'

K = typing.Optional[typing.Any]
V = typing.Any


def search(d: typing.Mapping[K, V], key: K, deep_search=False) -> typing.Iterator[typing.Tuple[typing.List[K], V]]:
    """Return a value corresponding to the specified key in the (possibly
    nested) dictionary d. If there is no item with that key, return
    default.
    """
    stack = [(None, iter(d.items()))]
    while stack:
        _, iterator = stack[-1]
        for path_element, value in iterator:
            if isinstance(value, dict) \
                or isinstance(value, collections.abc.Mapping) \
                    or isinstance(value, collections.abc.MutableMapping):
                stack.append((path_element, iter(value.items())))
                break
            if isinstance(value, list) \
                    or isinstance(value, collections.abc.MutableSequence):
                stack.append((path_element, enumerate(value)))
                break
            elif path_element == key:
                path = [el[0] for el in stack] + [path_element]
                if not deep_search:
                    stack.pop()
                yield (path[1:], value)
        else:
            stack.pop()


def get_schemaless_schema_pathes(schema: typing.Mapping[K, V]) -> typing.List[typing.List[typing.Union[str, int]]]:
    results = search(schema, SCHEMALESS_AVRO_SCHEMA)
    return [path[0] for path in results if path[1]]


def schema_path_to_object_path(schema: typing.Mapping[K, V], schema_path: typing.List[typing.Union[str, int]]) -> typing.List[str]:
    object_path = []
    schema_object = schema
    previous_path_elements = []
    for path_element in schema_path:
        if path_element == 'fields':
            previous_path_elements = [path_element]
        else:
            previous_path_elements.append(path_element)
        schema_object = schema_object[path_element]
        if path_element == 'items':
            object_path.append('[*]')
        elif len(previous_path_elements) == 2 and previous_path_elements[0] == 'fields':
            object_path.append(schema_object['name'])
            previous_path_elements = []
    return object_path


def get_schemaless_pathes(schema: typing.Mapping[K, V]) -> typing.List[typing.List[str]]:
    return [schema_path_to_object_path(schema, path) for path in get_schemaless_schema_pathes(schema)]


def get_schemaless_records(record, schemaless_pathes):
    schemaless_records = []
    for path in schemaless_pathes:
        records = [(record, None)]
        for path_element in path:            
            new_records = []
            for o,_ in records:
                if path_element == '[*]':
                    if isinstance(o, typing.Mapping):
                        for k, v in o.items():
                            if v:
                                new_records.append(
                                    (
                                        v,
                                        (lambda o, k: lambda x: o.__setitem__(k, x))(o,k)
                                    ))
                    if isinstance(o, typing.Iterable):
                        for idx, i in enumerate(o):
                            if i:
                                new_records.append(
                                    (
                                        i,
                                        (lambda o, k: lambda x: o.__setitem__(k, x))(o, idx)
                                    ))
                else:
                    if isinstance(o, typing.Mapping):
                        if path_element in o and o[path_element]:
                            new_records.append(
                                (
                                    o[path_element],
                                    (lambda o, k: lambda x: o.__setitem__(k, x))(o, path_element)
                                ))
                    else:
                        raise NotImplementedError("Access to list elements not implemented")
            records = new_records
        schemaless_records = schemaless_records + records
    return schemaless_records


def get_schemaless_avro_schema(namespace=None):
    """
    Generates an avro schema (as python object) suitable for storing arbitrary
    python nested data structure.
    For fastavro::
        schema.parse_schema(schemaless_avro_schema())
    For avro use::
        avro.schema.SchemaFromJSONData(schemaless_avro_schema())
    See also::
        to_schemaless_avro_destructive()
        from_schemaless_avro_destructive()
    :param namespace: if you would like to specify the namespace
    :return: python representation of avro schema
    """
    primitive_types = [PYTHON_TYPE_TO_AVRO[python_type]
                       for python_type in PRIMITIVE_AND_LOGICAL_TYPES]
    schema = OrderedDict(
        __schemaless_avro_schema__=True,
        type='record',
        # using a named record is the only way to make a
        # recursive structure in avro, so a nested map becomes {'_': {}}
        # nested list becomes {'_': []}
        # because it is an avro record, the storage is efficient
        name=SCHEMALESS_AVRO_SCHEMA_NAME,
        fields=[OrderedDict(
            name='_',
            type=[
                OrderedDict(type='map', values=primitive_types + [SCHEMALESS_AVRO_SCHEMA_NAME]),
                OrderedDict(type='array', items=primitive_types + [SCHEMALESS_AVRO_SCHEMA_NAME])
            ]
        )]
    )
    if namespace:
        schema['namespace'] = namespace
    return schema


def to_schemaless_avro_destructive(o, schema_gen=None, schemaless_pathes=[], types_to_str=()):
    """
    Converts a python nested data structure and returns a data structure
    in schemaless_avro format. The input data structure is destroyed and reused.
    schemaless_avro format is conforming to the avro schema generated by schemaless_avro_schema()
    For fastavro::
        fastavro.writer(
            out_stream,
            schema.parse_schema(schemaless_avro_schema),
            (to_schemaless_avro_destructive(record) for record in input_records))
    See also::
        schemaless_avro_schema()
        from_schemaless_avro_destructive()
    :param o: a data structure in schemaless_avro format.
    :param types_to_str: values of these types will be converted to str
        in the output data structure.
        Note that this is irreversible, i.e. they will be read back as strings.
    :return: python data structure in schemaless_avro format
    """
    if schema_gen:
       schemaless_pathes = schema_gen.get_schemaless_pathes()
    if schemaless_pathes:
        for record_with_setter in get_schemaless_records(o, schemaless_pathes):
            record = record_with_setter[0]
            setter = record_with_setter[1]
            record = to_schemaless_avro_destructive(record)
            setter(record) # pylint: disable=E1102
        return o

    if isinstance(o, str):
        return o

    if isinstance(o, types_to_str) and types_to_str:
        return str(o)

    if isinstance(o, Mapping):
        if isinstance(o, MutableMapping):
            for k, v in o.items():
                o[k] = to_schemaless_avro_destructive(v, types_to_str)
            return {'_': o}
        else:
            return {'_': {
                k: to_schemaless_avro_destructive(v, types_to_str)
                for k, v in o.items()}
            }

    if isinstance(o, Iterable):
        if isinstance(o, MutableSequence):
            for i in range(len(o)):
                o[i] = to_schemaless_avro_destructive(o[i], types_to_str)
            return {'_': o}
        else:
            return {'_': [to_schemaless_avro_destructive(i, types_to_str) for i in o]}

    return o


def from_schemaless_avro_destructive(o, schema_gen=None, schemaless_pathes=[]):
    """
    Converts a nested data structure in schemaless_avro format into a python nested
    data structure. The input data structure is destroyed and reused.
    schemaless_avro format is conforming to the avro schema generated by schemaless_avro_schema()
    For fastavro::
        records = [from_schemaless_avro_destructive(record)
                   for record in fastavro.reader(in_stream)]
    See also::
        schemaless_avro_schema()
        to_schemaless_avro_destructive()
    :param o: data structure in schemaless_avro format.
    :return: plain python data structure
    """
    if schema_gen:
       schemaless_pathes = schema_gen.get_schemaless_pathes()
    if schemaless_pathes:
        for record_with_setter in get_schemaless_records(o, schemaless_pathes):
            record = record_with_setter[0]
            setter = record_with_setter[1]
            record = from_schemaless_avro_destructive(record)
            setter(record) # pylint: disable=E1102
        return o

    if isinstance(o, str):
        return o

    if isinstance(o, Mapping):
        o = o['_']
        if isinstance(o, Mapping):
            if isinstance(o, MutableMapping):
                for k, v in o.items():
                    o[k] = from_schemaless_avro_destructive(v)
                return o
            else:
                return {k: from_schemaless_avro_destructive(v) for k, v in o.items()}
        if isinstance(o, Iterable):
            if isinstance(o, MutableSequence):
                for i in range(len(o)):
                    o[i] = from_schemaless_avro_destructive(o[i])
                return o
            else:
                return [from_schemaless_avro_destructive(i) for i in o]

        raise Exception('schemaless_object {"_": val} val must be Mapping or Iterable')

    return o
