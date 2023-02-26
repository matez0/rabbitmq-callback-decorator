from abc import ABC, abstractmethod
from functools import singledispatchmethod
import json
from typing import Union

from pydantic import BaseModel, ValidationError


class ErrorHandlerCallback(ABC):
    """Decorates with error handling layer."""

    class FatalError(Exception):
        """Rejects the message because it cannot be processed."""
        pass

    class TryAgainError(Exception):
        """Resends the message later to try to process it again."""
        pass

    def __init__(self, do_callback):
        self.do_callback = do_callback

    def __call__(self, channel, method_frame, header_frame, body):
        try:
            self.do_callback(body, header_frame.headers)

        except self.FatalError:
            self.reject_message(channel, method_frame, header_frame, body)

        except self.TryAgainError:
            self.resend_message_later(channel, method_frame, header_frame, body)

        else:
            self.acknowledge_message(channel, method_frame, header_frame, body)

    @staticmethod
    @abstractmethod
    def reject_message(channel, method_frame, header_frame, body):
        pass

    @staticmethod
    @abstractmethod
    def resend_message_later(channel, method_frame, header_frame, body):
        pass

    @staticmethod
    @abstractmethod
    def acknowledge_message(channel, method_frame, header_frame, body):
        pass


class JsonCallback:
    def __init__(self, do_callback):
        self.do_callback = do_callback

    def __call__(self, body, headers):
        try:
            body = json.loads(body)

        except json.JSONDecodeError as exc:
            raise ErrorHandlerCallback.FatalError(exc)

        self.do_callback(body, headers)


class ModelCallback:
    """Decorates with message parsing layer."""

    def __init__(self, *message_types):
        self.message_types = message_types

    def __call__(self, do_callback):
        def wrapper(body, headers):
            class Messages(BaseModel):
                message: Union[self.message_types]

            try:
                body = Messages(message=body).message

            except ValidationError as exc:
                raise ErrorHandlerCallback.FatalError(exc)

            do_callback(body, headers)

        return wrapper


class CallbackMeta(type(ABC)):
    """Decorates with message parsing layer, if arguments are given."""

    @singledispatchmethod
    def __call__(cls, do_callback: callable):
        return super().__call__(do_callback)

    @__call__.register
    def _(cls, message_type: type(BaseModel), *message_types):
        factory = super().__call__

        def wrap(do_callback):
            return factory(JsonCallback(ModelCallback(message_type, *message_types)(do_callback)))

        return wrap


class Callback(ErrorHandlerCallback, metaclass=CallbackMeta):
    """Decorates the callback function with error handling and message parsing layers.

    To create a decorator, derive from the class implementing the abstract methods.
    Example:

    >>> class MyCallback(Callback):
    ...     @staticmethod
    ...     def reject_message(channel, method_frame, header_frame, body):
    ...         channel.basic_reject(method_frame.delivery_tag)
    ...     @staticmethod
    ...     def resend_message_later(channel, method_frame, header_frame, body):
    ...         pass
    ...     @staticmethod
    ...     def acknowledge_message(channel, method_frame, header_frame, body):
    ...         channel.basic_ack(method_frame.delivery_tag)

    Without decorator parameters, the callback function will get the original message body and the headers.
    Example:

    >>> @MyCallback
    ... def do_my_callback(body, headers):
    ...     ...

    With decorator parameters, the parsed message and the headers are passed.
    The value of the arguments has to be a type of `BaseModel`.
    Example:

    >>> from pydantic import BaseModel
    >>>
    >>>
    >>> class MyModel(BaseModel):
    ...     my_field: int
    ...
    >>>
    >>> @MyCallback(MyModel)
    ... def do_my_callback(my_object, headers):
    ...     ...

    On error, raise `Callback.TryAgainError` or `Callback.FatalError` in the callback function.
    """
    pass
