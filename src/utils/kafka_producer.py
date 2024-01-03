import json
from aiokafka import AIOKafkaProducer
import asyncio

class AsyncKafkaProducer:
    def __init__(self,  port, host ='localhost', loop=None):
        self.host = host
        self.port = port

        self.bootstrap_servers = f"{self.host}:{self.port}"
        self.loop = loop or asyncio.get_event_loop()

        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.bootstrap_servers,
        )

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()
        print("Kafka Connection closed")

    async def push_to_kafka(self,topic, message):
        try:
            # Produce message to Kafka topic
            await self.producer.send(topic, json.dumps(message).encode('utf-8'))
            print(f"Message sent to {topic}: {message}")
        except Exception as e:
            print(f"Error while pushing message to Kafka: {e}")




# async def test_kafka_producer():
#     # Specify the Kafka broker host and port
#     kafka_host = 
#     kafka_port = 29092  # Change this to the actual port number

#     # Create an instance of AsyncKafkaProducer
#     kafka_producer = AsyncKafkaProducer(port=kafka_port, host=kafka_host)

#     try:
#         # Start the Kafka producer
#         await kafka_producer.start()

#         # Specify the Kafka topic and message to send
#         kafka_topic = 'your_topic'
#         kafka_message = {'key': 'value'}

#         # Send a message to Kafka
#         await kafka_producer.push_to_kafka(kafka_topic, kafka_message)

#     finally:
#         # Stop the Kafka producer (even if an exception occurs)
#         await kafka_producer.stop()

# if __name__ == "__main__":
#     # Run the test function
#     asyncio.run(test_kafka_producer())


