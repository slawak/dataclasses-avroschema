from faust.models import FieldDescriptor
from faust.exceptions import ValidationError

from faust.types.models import T

from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Type,
    TypeVar,
    List,
    cast,
)


class AvroFieldDescriptor(FieldDescriptor[T]):
    '''
    A custom FieldDescriptor to use for polymorphic fields with faust.
    
    Examples:
        >>> class Asset(Record):
        ...     url: str
        ...     type: str

        ... class ImageAsset(Asset):
        ...     type = 'image'

        ... class VideoAsset(Asset):
        ...     runtime_seconds: float
        ...     type = 'video'

        ... class Article(Record, polymorphic_fields=True):
        ...     assets: List[Asset] = AvroFieldDescriptor(avro_type = List[Union[ImageAsset, VideoAsset]])
    
    Arguments:
        avro_type (Type): Field value type for use in schema generation.        
    '''
    def __init__(self, avro_type: Type, **kwargs: Any) -> None:
        self.avro_type = avro_type
        # Must pass any custom args to init,
        # so we pass the choices keyword argument also here.
        super().__init__(avro_type=avro_type, **kwargs)

class EnumFieldDescriptor(FieldDescriptor[str]):
    '''
    A custom FieldDescriptor to use for enum fields with faust.
    
    Examples:
        >>> class Article(Record):
        ...     type: str = EnumFieldDescriptor(symbols = ["LONG","SHORT"])
    
    Arguments:
        symbols (List[str]): List of possible values.        
    '''
    def __init__(self, symbols: List[str], **kwargs: Any) -> None:
        self.symbols = symbols
        # Must pass any custom args to init,
        # so we pass the choices keyword argument also here.
        super().__init__(symbols=symbols, **kwargs)

    def validate(self, value: str) -> Iterable[ValidationError]:
        if value not in self.symbols:
            symbols = ', '.join(self.symbols)
            yield self.validation_error(
                f'{self.field} must be one of {symbols}')
