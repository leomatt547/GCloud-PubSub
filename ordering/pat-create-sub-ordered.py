# Creating subscription with ordering enabled

from google.cloud import pubsub_v1

# TODO(developer): Choose an existing topic.
project_id = "loyal-oath-309311"
topic_id = "pat-topic"
subscription_id = "pat-sub-2"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(project_id, topic_id)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

with subscriber:
    subscription = subscriber.create_subscription(
        request={
            "name": subscription_path,
            "topic": topic_path,
            "enable_message_ordering": True,
        }
    )
    print(f"Created subscription with ordering: {subscription}")