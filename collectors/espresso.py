import json
from pybeansack.utils import now
from azure.servicebus import ServiceBusClient
from collectors.individual import extract_source
from pybeansack.datamodels import BLOG, K_CREATED, K_SOURCE, K_UPDATED, K_URL, Bean

def collect(sb_conn_str: str, store_func):
    collected = now()
    with ServiceBusClient.from_connection_string(sb_conn_str).get_queue_receiver(queue_name="index-queue") as receiver:
        for _ in range(100): # just a random max loop count
            received_msgs = receiver.receive_messages(max_message_count=2000, max_wait_time=1) # random number of batch count
            if not received_msgs:
                break
            store_func([makebean(json.loads(str(msg)), collected) for msg in received_msgs])
            [receiver.complete_message(msg) for msg in received_msgs]

def makebean(data: dict, collected: int):
    return Bean(
        url=data[K_URL], 
        created=data.get(K_CREATED) or data.get(K_UPDATED) or collected, 
        updated=collected, 
        source=extract_source(data[K_URL])[0] or data[K_SOURCE],
        kind=BLOG)
