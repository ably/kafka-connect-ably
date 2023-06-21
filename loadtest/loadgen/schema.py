"""A JSON schema definition and code to push it to
the schema registry for testing purposes.
"""

from logging import getLogger

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from . import data


log = getLogger(__name__)


TEST_SCHEMA = """
{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "TestMessage",
    "description": "TestMessage for load testing Ably Connector",
    "type": "object",
    "properties": {
        "userId": {"type": "string"},
        "eventType": {"type": "string"},
        "payload": {"type": "string"}
    }
}
"""


def serializer(
    topic: str,
    schema_registry_url: str
) -> None:
    "Construct a JSON Serializer for the test schema"
    client = SchemaRegistryClient({'url': schema_registry_url})
    json_serializer = JSONSerializer(
        TEST_SCHEMA,
        client,
        data.test_message_as_dict
    )
    ctx = SerializationContext(topic, MessageField.VALUE)
    return lambda msg: json_serializer(msg, ctx)
