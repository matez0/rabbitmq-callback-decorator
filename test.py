import json
from unittest import TestCase
from unittest.mock import Mock, patch

from pydantic import BaseModel, NegativeInt

from callback_decorator import Callback


class TestCallbackDecoratorBase(TestCase):
    def setUp(self):
        class DerivedCallback(Callback):
            reject_message = Mock()
            resend_message_later = Mock()
            acknowledge_message = Mock()

        self.callback_function = Mock()

        self.callback = DerivedCallback(self.callback_function)

        self.headers = {'header-key': 'header-value'}
        self.body = '"body"'

        self.callback_args = ('channel', 'method-frame', Mock(headers=self.headers), self.body)


class TestErrorHandlerCallbackDecorator(TestCallbackDecoratorBase):
    def test_callback_class_can_be_used_as_decorator(self):
        decorator = type(self.callback)

        @decorator
        def callback_function(*args):
            self.callback_function(*args)

        callback_function(*self.callback_args)

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


@patch('callback_decorator.json.loads')
class TestModelCallbackDecorator(TestCallbackDecoratorBase):
    class MyMessage(BaseModel):
        myField: float

    def setUp(self):
        super().setUp()

        decorator = type(self.callback)

        @decorator(self.MyMessage)
        def callback(*args):
            self.callback_function(*args)

        self.callback = callback

        self.message = b'{"myField": 3.14}'
        self.callback_args = *self.callback_args[:3], self.message

    def test_parsed_message_is_passed_when_message_type_is_given(self, json_loads):
        parsed_message = self.MyMessage.parse_raw(self.message)
        json_loads.return_value = parsed_message.dict()

        self.callback(*self.callback_args)

        self.callback_function.assert_called_once_with(parsed_message, self.headers)

        self.callback.acknowledge_message.assert_called_once_with(*self.callback_args)

    def test_parsed_message_is_passed_when_multiple_message_types_are_given(self, json_loads):
        decorator = type(self.callback)

        class MyFirstMessage(BaseModel):
            myFirstField: bool

        class MyLastMessage(BaseModel):
            myLastField: NegativeInt

        @decorator(MyFirstMessage, self.MyMessage, MyLastMessage)
        def callback(*args):
            self.callback_function(*args)

        parsed_message = self.MyMessage.parse_raw(self.message)
        json_loads.return_value = parsed_message.dict()

        callback(*self.callback_args)

        self.callback_function.assert_called_once_with(parsed_message, self.headers)

        self.callback.acknowledge_message.assert_called_once_with(*self.callback_args)

    def test_message_shall_be_rejected_when_it_is_not_json(self, json_loads):
        json_loads.side_effect = json.JSONDecodeError('error-message1', 'doc', 123)

        self.callback(*self.callback_args)

        self.callback_function.assert_not_called()

        json_loads.assert_called_once_with(self.message)

        self.callback.acknowledge_message.assert_not_called()
        self.callback.reject_message.assert_called_once_with(*self.callback_args)
        self.callback.resend_message_later.assert_not_called()

    def test_message_shall_be_rejected_when_it_is_not_parsable(self, json_loads):
        message = b'{"myOtherField": "my-value"}'
        json_loads.return_value = {"myOtherField": "my-value"}

        callback_args = *self.callback_args[:3], message

        self.callback(*callback_args)

        self.callback_function.assert_not_called()

        self.callback.acknowledge_message.assert_not_called()
        self.callback.reject_message.assert_called_once_with(*callback_args)
        self.callback.resend_message_later.assert_not_called()
