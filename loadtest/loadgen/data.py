"""Test data schema and generators
"""

import random
import string

import dataclasses as dc


# Make simulations repeatable
random.seed(1234)


@dc.dataclass
class TestMessage:
    "Test message type, must match above JSON schema"
    userId: str
    eventType: str
    payload: str

    @classmethod
    def rand(cls,
             n_users: int,
             n_types: int,
             max_message_size: int
             ) -> 'TestMessage':
        types = [f'type-{x}' for x in range(n_types)]
        payload_size = random.randint(1, max_message_size)
        return TestMessage(
            userId=f'user-{random.randint(1, n_users)}',
            eventType=random.choice(types),
            payload=''.join(
                random.choice(string.ascii_letters)
                for _ in range(payload_size)
            )
        )


def test_message_as_dict(msg: TestMessage, _):
    "Convert a TestMessage instance to a dictionary"
    return dc.asdict(msg)
