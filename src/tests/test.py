# from datetime import datetime

# # Example timestamp in milliseconds
# timestamp_milliseconds = 1699436696000  # Replace this with your actual timestamp
# PERIOD_TO_GET = 60 * 60 * 24 * 3

# # Convert milliseconds since the epoch to seconds
# timestamp_seconds = timestamp_milliseconds / 1000.0

# # Convert timestamp to datetime object
# datetime_object = datetime.utcfromtimestamp(timestamp_seconds)
# datetime_now =datetime.now()

# difference = (datetime_now - datetime_object).total_seconds()
# print(difference > PERIOD_TO_GET)



# print(datetime_object, type(datetime_object))


# def synchronous_function():
#     print("Start")
#     result = add_numbers(1, 2)
#     print("Result:", result)
#     print("End")

# def add_numbers(a, b):
#     # Simulate a time-consuming operation
#     import time
#     time.sleep(2)
#     return a + b

# synchronous_function()

import time
import asyncio

def async_timing_decorator(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{func.__name__} took {execution_time:.4f} seconds to execute.")
        return result
    return wrapper




# @async_timing_decorator
# async def asynchronous_function():
#     print("Start")
#     result, result2 = await asyncio.gather(add_numbers(1, 2), add_numbers(1, 3))
#     print("Result:", result)
#     print("Result:", result2)
#     print("End")


# async def add_numbers(a, b):
#     # Simulate a time-consuming operation
#     await asyncio.sleep(2)
#     return a + b

# # Create an event loop and run the asynchronous function
# asyncio.run(asynchronous_function())

# from time import sleep

# async def foo():
#     await asyncio.sleep(2)
#     return "Foo result"

# async def bar():
#     sleep(1)
#     return "Bar result"

# @async_timing_decorator
# async def main():
#     results = await asyncio.gather(foo(), bar())
#     print(results)

# # Run the event loop
# asyncio.run(main())



# from kafka import KafkaProducer, KafkaConsumer

# from time import sleep

# # Replace these values with your Kafka bootstrap servers and topic
# bootstrap_servers = 'host.docker.internal:29092'
kafka_topic = 'test'

# # Configuration for PLAINTEXT
# plaintext_config = {'bootstrap_servers': bootstrap_servers}

# try:
#     # Create Kafka Producer instance
#     producer = KafkaProducer(**plaintext_config)
#     # consumer  = KafkaConsumer(kafka_topic, **plaintext_config)
#     # Produce a test message to the Kafka topic
#     message_value = b'Hello, Kafka!'
    # for i in range(1,20):
    #     producer.send(kafka_topic, value=message_value)

    #     print(f"Message sent to {kafka_topic}: {message_value.decode('utf-8')}")

#     # for message in consumer:
#     #     print(f"Received message: {message.value.decode('utf-8')}")

# finally:
#     # Close the Kafka producer
#     producer.close()

from utils.kafka_producer import AsyncKafkaProducer


# from aiokafka import AIOKafkaProducer
# import asyncio

# async def send_one():
#     producer = AIOKafkaProducer(bootstrap_servers='localhost:29092')
#     # Get cluster layout and initial topic/partition leadership information
#     await producer.start()
#     try:
#         # Produce message
#         message_value = b'Hello, Kafka!'
#         for i in range(1,100):
#             await producer.send_and_wait(kafka_topic, message_value)

#             print(f"Message sent to {kafka_topic}: {message_value.decode('utf-8')}")
            
#     finally:
#         # Wait for all pending messages to be delivered or expire.
#         await producer.stop()

# asyncio.run(send_one())