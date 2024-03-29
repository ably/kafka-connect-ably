"""Kafka producer to simulate message load on a topic.
"""


from asyncio import Task, sleep
from logging import getLogger
from typing import Iterable, List

import random

from aiokafka import AIOKafkaProducer

from .data import TestMessage

log = getLogger(__name__)


def rand_messages(
    user_id: int,
    n_types: int,
    max_payload_size: int,
    n_messages: int
) -> Iterable[TestMessage]:
    "Generate a sequence of random TestMessages"
    for seq in range(0, n_messages):
        yield TestMessage.rand(seq, user_id, n_types, max_payload_size)


def jitter(delay: float):
    "Jitter the message sending delay to space out publishing"
    magnitude = 0.2 * delay
    return delay + random.uniform(delay - magnitude/2.0, delay + magnitude/2.0)


async def producer(
    kafka_producer: AIOKafkaProducer,
    topic: str,
    messages: Iterable[TestMessage],
    message_delay_secs: float
) -> None:
    """Send message sequence to Kafka asynchronously, with delay
    between messages"""
    for msg in messages:
        delay = jitter(message_delay_secs)
        log.debug(f"Sleeping for {delay} seconds")
        await sleep(delay)
        log.debug(f"Sending: {msg}")
        await kafka_producer.send(
            topic,
            key=msg.userId.encode('utf-8'),
            value=msg
        )


def make_producers(
    kafka_producer: AIOKafkaProducer,
    topic: str,
    n_users: int,
    total_messages_per_user: int,
    message_delay_secs: float,
    max_message_size: int,
    n_message_types: int,
) -> List[Task]:
    """Return a list of async tasks that will send randomized
    message data to the given Kafka topic when executed
    """
    return [
        producer(
            kafka_producer,
            topic,
            rand_messages(user_id, n_message_types,
                          max_message_size, total_messages_per_user),
            message_delay_secs
        )
        for user_id in range(n_users)
    ]
