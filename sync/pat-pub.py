from google.cloud import pubsub_v1

#set GOOGLE_APPLICATION_CREDENTIALS=auth.json
# TODO(developer)
project_id = "loyal-oath-309311"
topic_id = "pat-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

while(1):
    data = input("Masukkan pesan:")
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(future.result())
    print(f"Published messages to {topic_path}.")