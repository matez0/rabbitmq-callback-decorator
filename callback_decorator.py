from abc import ABC, abstractmethod


class Callback(ABC):
    class FatalError(Exception):
        pass

    class TryAgainError(Exception):
        pass

    def __init__(self, do_callback):
        self.do_callback = do_callback

    def __call__(self, channel, method_frame, header_frame, body):

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
