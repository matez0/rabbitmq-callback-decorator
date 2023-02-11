from unittest import TestCase
from unittest.mock import Mock

from callback_decorator import Callback


class TestCallbackDecorator(TestCase):
    def setUp(self):
        class DerivedCallback(Callback):
            reject_message = Mock()
            resend_message_later = Mock()
            acknowledge_message = Mock()

        self.callback_function = Mock()

        self.callback = DerivedCallback(self.callback_function)

        self.headers = {'header-key': 'header-value'}
        self.body = 'body'

        self.callback_args = ('channel', 'method-frame', Mock(headers=self.headers), self.body)

    def test_callback_class_can_be_used_as_decorator(self):
        decorator = type(self.callback)

        @decorator
        def callback_function(*args):
            self.callback_function(*args)

        self.callback(*self.callback_args)

        self.callback_function.assert_called_once_with(self.body, self.headers)

    def test_message_is_acknowledged_when_callback_raises_no_exception(self):
        self.callback(*self.callback_args)

        self.callback_function.assert_called_once_with(self.body, self.headers)

        self.callback.acknowledge_message.assert_called_once_with(*self.callback_args)
        self.callback.reject_message.assert_not_called()
        self.callback.resend_message_later.assert_not_called()

    def test_message_is_rejected_when_callback_raises_fatal_error(self):
        self.callback_function.side_effect = Callback.FatalError()

        self.callback(*self.callback_args)

        self.callback_function.assert_called_once_with(self.body, self.headers)

        self.callback.acknowledge_message.assert_not_called()
        self.callback.reject_message.assert_called_once_with(*self.callback_args)
        self.callback.resend_message_later.assert_not_called()

    def test_message_is_resent_later_when_callback_raises_try_again_error(self):
        self.callback_function.side_effect = Callback.TryAgainError()

        self.callback(*self.callback_args)

        self.callback_function.assert_called_once_with(self.body, self.headers)

        self.callback.acknowledge_message.assert_not_called()
        self.callback.reject_message.assert_not_called()
        self.callback.resend_message_later.assert_called_once_with(*self.callback_args)
