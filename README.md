[![test](https://github.com/matez0/rabbitmq-callback-decorator/actions/workflows/test.yml/badge.svg)](https://github.com/matez0/rabbitmq-callback-decorator/actions)
[![Python versions](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![license](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

# Message parsing and error handling decorator for RabbitMQ (AMQP 0-9-1) consumer callbacks

The decorator organizes the basic error handling of the message queuing protocol and the message parsing
into a separate layer from the callback function.

The message parsing uses [Pydantic](https://docs.pydantic.dev/).

## Usage

Derive from the abstract decorator implementing the abstract methods:

```python
>>> from callback_decorator import Callback
>>>
>>>
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

```

Create your custom data model:

```python
>>> from pydantic import BaseModel
>>>
>>>
>>> class MyModel(BaseModel):
...     my_field: int

```

Decorate your callback function:

```python
>>> @MyCallback(MyModel)
... def do_my_callback(my_object, headers):
...     ...

```

On error, raise one of the following exceptions:
- `Callback.FatalError`, if the message can never be processed;
- `Callback.TryAgainError`, if it is worth trying to process the message later.
