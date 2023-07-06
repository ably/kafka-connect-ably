"""Ably Realtime subscriber to receive messages coming
through Ably channels and assert that received data looks
correct.
"""

from asyncio import Task, locks, wait_for
from logging import getLogger
from typing import List

from ably import AblyRealtime
from ably.types.message import Message

from .data import TestMessage


log = getLogger(__name__)


async def consumer(
    client: AblyRealtime,
    user_id: int,
    n_messages_expected: int,
    check_order: bool
) -> None:
    """Construct a consumer worker to receive Ably messages using a Realtime
    connection and optionally assert that the expected number of messages
    have arrived in the correct order.
    """
    channel = client.channels.get(f'user:user-{user_id}')

    finished = locks.Event()
    n_received = 0
    last_seq = -1
    seq_sum = 0

    def listener(msg: Message):
        nonlocal n_received, last_seq, seq_sum, finished
        test_message = TestMessage(**msg.data)
        log.debug(f'{user_id}: message received: {test_message}')

        n_received += 1
        seq_sum += test_message.sequence

        if check_order and (last_seq > test_message.sequence):
            log.error(f'{user_id} received message ' +
                      f'{test_message} after sequence={last_seq}')

        if n_received == n_messages_expected:
            finished.set()

    await channel.subscribe(listener)
    await finished.wait()

    expected_seq_sum = (n_messages_expected * (n_messages_expected - 1)) / 2
    if seq_sum != expected_seq_sum:
        log.error(f'{user_id}: finished with unexpected seq_sum: ' +
                  f'seq_sum={seq_sum}, expected={expected_seq_sum}')
    else:
        print(f'{user_id} completed. ' +
              f'n_received = {n_received} / {n_messages_expected}, ' +
              f'seq_sum = {seq_sum} / {expected_seq_sum}')


def make_consumers(
    client_key: str,
    n_channels: int,
    messaged_per_worker: int,
    timeout_seconds: float,
    check_order: bool
) -> List[Task]:
    """Build a list of consumer corotuines, with maximum running durations,
    to collect Ably messages on test channels and check incoming data.
    """
    client = AblyRealtime(client_key)
    return [
        wait_for(
            consumer(client, user_id, messaged_per_worker, check_order),
            timeout_seconds
        )
        for user_id in range(n_channels)
    ]
