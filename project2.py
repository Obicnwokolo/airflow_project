import requests
import json
import time
import hashlib
from kafka import KafkaProducer


API_url = "https://randomuser.me/api/?results=1"

response = requests.get(API_url)
print(response)
retrieved_user_data = response.json()['results'][0]
#print(user_data)

# def transform_user_data(data: dict) -> dict:
#     """Formats the fetched user data for Kafka streaming."""
#     return{
#         "name": f"{data['name']['title']}.{data['name']['first']}{data['name']['last']}",
#         "gender": data["gender"],
#         "address": f"{data['location']['street']['number']}, {data['location']['street']['name']}",  
#         "city": data['location']['city'],
#         "nation": data['location']['country'],  
#         "zip": (data['location']['postcode']),  
#         "latitude": float(data['location']['coordinates']['latitude']),
#         "longitude": float(data['location']['coordinates']['longitude']),
#         "email": data["email"]
#     }

# formatted_user_data = transform_user_data(retrieved_user_data)
# print(formatted_user_data )

KAFKA_BOOT_STRAP_services= ["ip-172-31-8-235.eu-west-2.compute.internal:9092","ip-172-31-14-3.eu-west-2.compute.internal:9092","ip-172-31-1-36.eu-west-2.compute.internal:9092"]
KAFKA_TOPIC = 'tesT_obinna.'
PAUSE_INTERVAL = 10
STREAMING_DURATION = 120

def configure_kafka(servers = KAFKA_BOOT_STRAP_services):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': ','.join(servers),
        'client.id': 'producer_instance'
    }
    return KafkaProducer(settings)

def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_status)
    producer.flush()


def delivery_status(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        print('Message deliverey failed:', err)
    else:
        print('Message delivered to', msg.topic(), '[partition: {}]'.format(msg.partition()))

def initiate_stream():
    """Initiates the process to stream user data to Kafka."""
    kafka_producer = configure_kafka()
    for _ in range(STREAMING_DURATION // PAUSE_INTERVAL):
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, retrieved_user_data)
        time.sleep(PAUSE_INTERVAL)

if __name__ == "__main__":
    initiate_stream()
