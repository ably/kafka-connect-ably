"""Main entry point
"""

from argparse import ArgumentParser
import asyncio
import logging

from aiokafka import AIOKafkaProducer

from . import schema, producer


def kafka_producer(
    bootstrap_servers: str,
    topic: str,
    schema_registry_url: str
) -> AIOKafkaProducer:
    "Return a Kafka Producer instance, configured for testing"
    serializer = schema.serializer(topic, schema_registry_url)
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id='AblyLoadTest',
        value_serializer=serializer
    )


async def main(args):
    # Create producer tasks and wait for them to complete
    try:
        prod = kafka_producer(
            args.kafka_bootstrap_servers,
            args.topic,
            args.schema_registry_url
        )
        await prod.start()
        tasks = producer.make_producers(
            prod,
            args.topic,
            args.n_channels,
            args.messages_per_worker,
            args.message_send_delay,
            args.max_message_size,
            args.n_message_names,
            args.n_workers
        )
        await asyncio.gather(*tasks)
    finally:
        await prod.stop()


if __name__ == '__main__':
    parser = ArgumentParser(
        description="Simulate load for Ably Kafka Connector"
    )
    parser.add_argument(
        '--kafka-bootstrap-servers',
        help='host:port for kafka bootstrap servers to connect to',
        type=str,
        default='localhost:9092'
    )
    parser.add_argument(
        '--n-workers',
        help='Number of concurrent publishers (to Kafka)',
        type=int,
        default=8
    )
    parser.add_argument(
        '--topic',
        help='Kafka topic to publish to',
        default="ably-connector-test-topic"
    )
    parser.add_argument(
        '--n-channels',
        help='Number of destination Ably channels',
        type=int,
        default=100
    )
    parser.add_argument(
        '--messages-per-worker',
        help='Total number of messages to send to each channel',
        type=int,
        default=10
    )
    parser.add_argument(
        '--message-send-delay',
        help='Delay (in seconds) between each publish (per worker)',
        type=float,
        default=0.25
    )
    parser.add_argument(
        '--max-message-size',
        help='Messages will contain a random payload up to this many bytes',
        type=int,
        default=1024
    )
    parser.add_argument(
        '--n-message-names',
        help='Number of distinct message names to use',
        type=int,
        default=10
    )
    parser.add_argument(
        '--schema_registry_url',
        help='URL to the Schema regsitry being used by Kafka Connect',
        default="http://localhost:8081"
    )
    parser.add_argument(
        '-v',
        help='Enable verbose logging',
        default=False,
        action='store_true'
    )

    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=logging.DEBUG if args.v else logging.INFO
    )

    asyncio.run(main(args))
