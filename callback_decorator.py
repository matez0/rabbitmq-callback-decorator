from abc import ABC, abstractmethod
from functools import singledispatchmethod
import json
from typing import Union

from pydantic import BaseModel, ValidationError


class ErrorHandlerCallback(ABC):
    class FatalError(Exception):
        pass

    class TryAgainError(Exception):
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

    @abstractmethod
    def reject_message(channel, method_frame, header_frame, body):
        pass

    @abstractmethod
    def resend_message_later(channel, method_frame, header_frame, body):
        pass

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


MESSAGE_TYPE_HEADER = 'message-type'


class ModelCallback:
    def __init__(self, *message_types):
        self.message_types = message_types

    def __call__(self, do_callback):
        def wrapper(body, headers):
            if MESSAGE_TYPE_HEADER in headers:
                try:
                    message_types = ({
                        message_type.__name__: message_type for message_type in self.message_types
                    }[headers[MESSAGE_TYPE_HEADER]],)

                except KeyError as exc:
                    raise ErrorHandlerCallback.FatalError(exc)

            else:
                message_types = self.message_types

            class Messages(BaseModel):
                message: Union[message_types]

            try:
                body = Messages(message=body).message

            except ValidationError as exc:
                raise ErrorHandlerCallback.FatalError(exc)

            do_callback(body, headers)

        return wrapper


class CallbackMeta(type(ABC)):
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
    pass
