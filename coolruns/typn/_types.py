
from typing import get_type_hints, Union, Optional, get_origin, get_args, Any, Type
from collections.abc import Callable



def typedictclass(cls: Type, *, coerce: bool = False, strict: bool = True):
    """
    Create a type-checkable pseudo-class for validating dictionaries.
    This enables isinstance(dict, MyTypedDict) to return True only if:
    - The dict contains exactly the declared keys (or more, if strict=False)
    - The values match the expected types (with optional coercion)

    Arguments:
    - coerce: If True, will attempt to coerce values into expected types
    - strict: If True, keys must match exactly. If False, additional keys are allowed

    The class will also have a `.validate(dict)` method for runtime enforcement.
    """

    expected_fields = get_type_hints(cls)

    def _check_type(value: Any, expected_type: Any) -> bool:
        # Handle Any
        if expected_type is Any:
            return True

        origin = get_origin(expected_type)
        args = get_args(expected_type)

        # Handle Optional[X] (which is Union[X, None])
        if origin is Union and type(None) in args:
            remaining = tuple(a for a in args if a is not type(None))
            return value is None or isinstance(value, remaining)

        # Handle Union[A, B, C...]
        if origin is Union:
            return any(isinstance(value, arg) for arg in args)

        return isinstance(value, expected_type)

    def _coerce_value(value: Any, expected_type: Any) -> Any:
        origin = get_origin(expected_type)
        args = get_args(expected_type)

        if expected_type is Any:
            return value

        # Coerce for Optional[X] or Union[X, Y, ...]
        if origin is Union:
            last_error = None
            for arg in args:
                try:
                    if value is None and arg is type(None):
                        return None
                    return _coerce_value(value, arg)
                except Exception as e:
                    last_error = e
            raise last_error or ValueError(f"Cannot coerce value '{value}' into {expected_type}")

        # Normal coercion
        return expected_type(value)

    class TypedDictMeta(type):

        def __instancecheck__(self, instance: Any) -> bool:
            if not isinstance(instance, dict):
                return False

            if strict and set(instance.keys()) != set(expected_fields.keys()):
                return False
            if not strict and not set(expected_fields.keys()).issubset(instance.keys()):
                return False

            for key, expected_type in expected_fields.items():
                if key not in instance:
                    return False
                value = instance[key]
                if not _check_type(value, expected_type):
                    if not coerce:
                        return False
                    try:
                        _coerce_value(value, expected_type)
                    except Exception:
                        return False

            return True

        def validate(cls, obj: dict) -> dict:
            """
            Validates and optionally coerces a dictionary to match the declared schema.

            Raises:
                TypeError: if types are incompatible and coercion fails
                KeyError: if a required key is missing
            Returns:
                A new dictionary matching the expected types
            """
            if not isinstance(obj, dict):
                raise TypeError("Only dictionaries can be validated.")

            result = {}
            for key, expected_type in expected_fields.items():
                if key not in obj:
                    raise KeyError(f"Missing required key: '{key}'")
                value = obj[key]
                if _check_type(value, expected_type):
                    result[key] = value
                elif coerce:
                    result[key] = _coerce_value(value, expected_type)
                else:
                    raise TypeError(f"Incorrect type for key '{key}': expected {expected_type}, got {type(value)}")
            return result

    return TypedDictMeta(cls.__name__, (object,), dict(__annotations__=expected_fields))