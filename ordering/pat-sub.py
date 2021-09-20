from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

#set GOOGLE_APPLICATION_CREDENTIALS=auth.json
# TODO(developer)
project_id = "loyal-oath-309311"
subscription_id = "pat-sub"

A = []
B = []
C = []
# Number of seconds the subscriber should listen for messages
timeout = 5.0
subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    if(message.ordering_key == 'A'):
        A.append(message.data)
    elif(message.ordering_key == 'B'):
        B.append(message.data)
    elif(message.ordering_key == 'C'):
        C.append(message.data)
    message.ack()
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Menunggu pesan dari {subscription_path}..\n")
# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

print("Ordering Key A")
for i in range (len(A)):
    print(A[i])
print("")
print("Ordering Key B")
for j in range (len(B)):
    print(B[j])
print("")
print("Ordering Key C")
for k in range (len(C)):
    print(C[k])
print("")
