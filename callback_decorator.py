from abc import ABC, abstractmethod
import json
from typing import Union

from pydantic import BaseModel


class CallbackMeta(type(ABC)):
    def __call__(cls, arg, *args):
        factory = super().__call__

        def wrap(do_callback, message_types=(arg, *args)):
            instance = factory(do_callback)
            instance.message_types = message_types
            return instance

        return wrap if args else wrap(arg, ())


class Callback(ABC, metaclass=CallbackMeta):
    class FatalError(Exception):
        pass

    class TryAgainError(Exception):
        pass

    def __init__(self, do_callback):
        self.do_callback = do_callback

    def __call__(self, channel, method_frame, header_frame, body):
        if self.message_types:
            class Messages(BaseModel):
                message: Union[self.message_types]

            body = json.loads(body)
            body = Messages(message=body).message

        try:
            self.do_callback(body, header_frame.headers)

        except Callback.FatalError:
            self.reject_message(channel, method_frame, header_frame, body)

        except Callback.TryAgainError:
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
