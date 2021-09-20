# Publish ordered messages with (random) ordering key

from google.cloud import pubsub_v1
import random

# TODO(developer)
project_id = "i-hexagon-308502"
topic_id = "order-topic"

r = random.Random()

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):
    order = chr(r.randint(0, 2)+ord('A'))
    data = f"Message number {n} with ordering id {order}"
    print(data)
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data, ordering_key=order)
    print(future.result())

print(f"Published messages to {topic_path}.")