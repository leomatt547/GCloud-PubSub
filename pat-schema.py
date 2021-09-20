# Pub/Sub with schemas

import argparse
import json

from google.cloud import pubsub_v1

project_id = "i-hexagon-308502"

def create_schema(schema_id, avsc_file):
    """Create a schema from an .avsc file"""
    from google.api_core.exceptions import AlreadyExists
    from google.cloud.pubsub import SchemaServiceClient
    from google.pubsub_v1.types import Schema

    # TODO(developer): Replace these variables before running the sample.
    project_path = f'projects/{project_id}'

    # Read a JSON-formatted Avro schema file as a string.
    with open(avsc_file, "rb") as f:
        avsc_source = f.read().decode("utf-8")

    schema_client = SchemaServiceClient()
    schema_path = schema_client.schema_path(project_id, schema_id)
    schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=avsc_source)

    try:
        result = schema_client.create_schema(
            request={"parent": project_path, "schema": schema, "schema_id": schema_id}
        )
        print(f"Created a schema using an Avro schema file:\n{result}")
    except AlreadyExists:
        print(f"{schema_id} already exists.")


def create_topic_with_schema(schema_id, topic_id, message_encoding) -> None:
    """Create a topic resource with a schema."""
    # [START pubsub_create_topic_with_schema]
    from google.api_core.exceptions import AlreadyExists, InvalidArgument
    from google.cloud.pubsub import PublisherClient, SchemaServiceClient
    from google.pubsub_v1.types import Encoding

    # TODO(developer): Replace these variables before running the sample.
    publisher_client = PublisherClient()
    topic_path = publisher_client.topic_path(project_id, topic_id)

    schema_client = SchemaServiceClient()
    schema_path = schema_client.schema_path(project_id, schema_id)

    if message_encoding == "BINARY":
        encoding = Encoding.BINARY
    elif message_encoding == "JSON":
        encoding = Encoding.JSON
    else:
        encoding = Encoding.ENCODING_UNSPECIFIED

    try:
        response = publisher_client.create_topic(
            request={
                "name": topic_path,
                "schema_settings": {"schema": schema_path, "encoding": encoding},
            }
        )
        print(f"Created a topic:\n{response}")

    except AlreadyExists:
        print(f"{topic_id} already exists.")
    except InvalidArgument:
        print("Please choose either BINARY or JSON as a valid message encoding type.")
    # [END pubsub_create_topic_with_schema]


def publish(topic_id: str, avsc_file: str, record: dict) -> None:
    """Pulbish a BINARY or JSON encoded message to a topic configured with an Avro schema."""
    # [START pubsub_publish_avro_records]
    from avro.io import BinaryEncoder, DatumWriter
    import avro
    import io
    from google.api_core.exceptions import NotFound
    from google.cloud.pubsub import PublisherClient
    from google.pubsub_v1.types import Encoding

    publisher_client = PublisherClient()
    topic_path = publisher_client.topic_path(project_id, topic_id)

    # Prepare to write Avro records to the binary output stream.
    avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
    writer = DatumWriter(avro_schema)
    bout = io.BytesIO()

    try:
        # Get the topic encoding type.
        topic = publisher_client.get_topic(request={"topic": topic_path})
        encoding = topic.schema_settings.encoding

        # Encode the data according to the message serialization type.
        if encoding == Encoding.BINARY:
            encoder = BinaryEncoder(bout)
            writer.write(record, encoder)
            data = bout.getvalue()
            print(f"Preparing a binary-encoded message:\n{data}")
        elif encoding == Encoding.JSON:
            data = json.dumps(record).encode("utf-8")
            print(f"Preparing a JSON-encoded message:\n{data}")
        else:
            print(f"No encoding specified in {topic_path}. Abort.")
            exit(0)

        future = publisher_client.publish(topic_path, data)
        print(f"Published message ID: {future.result()}")

    except NotFound:
        print(f"{topic_id} not found.")
    except:
        print("Unknown error")
    # [END pubsub_publish_avro_records]


def receive(
        subscription_id: str, avsc_file: str, timeout: float = 5.0
) -> None:
    """Receive and decode messages sent to a topic with an Avro schema."""
    # [START pubsub_subscribe_avro_records]
    import avro
    from avro.io import BinaryDecoder, DatumReader
    from concurrent.futures import TimeoutError
    import io
    from google.cloud.pubsub import SubscriberClient

    subscriber = SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    avro_schema = avro.schema.parse(open(avsc_file, "rb").read())

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        # Get the message serialization type.
        encoding = message.attributes.get("googclient_schemaencoding")
        # Deserialize the message data accordingly.
        if encoding == "BINARY":
            bout = io.BytesIO(message.data)
            decoder = BinaryDecoder(bout)
            reader = DatumReader(avro_schema)
            message_data = reader.read(decoder)
            print(f"Received a binary-encoded message:\n{message_data}")
        elif encoding == "JSON":
            message_data = json.loads(message.data)
            print(f"Received a JSON-encoded message:\n{message_data}")
        else:
            print(f"Received a message with no encoding:\n{message}")

        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception occurs first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    # [END pubsub_subscribe_avro_records]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    create_schema_parser = subparsers.add_parser(
        "create-schema", help=create_schema.__doc__
    )
    create_schema_parser.add_argument("schema_id", help="Schema ID")
    create_schema_parser.add_argument("avsc_file", help="Path to .avsc file")

    create_topic_with_schema_parser = subparsers.add_parser(
        "create-topic", help=create_topic_with_schema.__doc__
    )
    create_topic_with_schema_parser.add_argument("topic_id", help="Topic ID")
    create_topic_with_schema_parser.add_argument("schema_id", help="Schema ID")
    create_topic_with_schema_parser.add_argument(
        "message_encoding", choices=["BINARY", "JSON"], help="Schema encoding"
    )

    publish_records_parser = subparsers.add_parser(
        "publish", help=publish.__doc__
    )
    publish_records_parser.add_argument("topic_id", help="Topic ID")
    publish_records_parser.add_argument("avsc_file", help="Path to .avsc file")
    publish_records_parser.add_argument("message", help="Message to publish (JSON), don't forget to escape the quotes")

    subscribe_parser = subparsers.add_parser(
        "receive", help=receive.__doc__
    )
    subscribe_parser.add_argument("subscription_id", help="Subscription ID")
    subscribe_parser.add_argument("avsc_file", help="Path to .avsc file")
    subscribe_parser.add_argument(
        "timeout", default=None, type=float, nargs="?", help="Timeout before closing. Default 5 seconds"
    )

    args = parser.parse_args()

    if args.command == "create-schema":
        create_schema(args.schema_id, args.avsc_file)
    if args.command == "create-topic":
        create_topic_with_schema(
            args.topic_id, args.schema_id, args.message_encoding
        )
    if args.command == "publish":
        publish(args.topic_id, args.avsc_file, json.loads(args.message))
    if args.command == "receive":
        receive(
            args.subscription_id, args.avsc_file, args.timeout
        )
