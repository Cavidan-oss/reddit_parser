import json
from aiokafka import AIOKafkaProducer
import asyncio

class AsyncKafkaProducer:
    def __init__(self, bootstrap_servers, loop=None):
        self.bootstrap_servers = bootstrap_servers
        self.loop = loop or asyncio.get_event_loop()

        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.bootstrap_servers,
        )

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()

    async def push_to_kafka(self,topic, message):
        try:
            # Produce message to Kafka topic
            await self.producer.send(topic, json.dumps(message).encode('utf-8'))
            print(f"Message sent to {topic}: {message}")
        except Exception as e:
            print(f"Error while pushing message to Kafka: {e}")
