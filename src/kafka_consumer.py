from kafka import KafkaConsumer
from utils.mongodb_helper import MongoDBHelper
from dotenv import dotenv_values
import json

env_vars = dotenv_values(".env") # Replace these values with your Kafka bootstrap servers and topic
print(env_vars)

# Configuration for PLAINTEXT
consumer = None
try:
    print(f"{env_vars.get('KAFKA_HOST')}:{env_vars.get('KAFKA_PORT')}")
    consumer = KafkaConsumer(
        'test1',
        group_id='consumers',
        bootstrap_servers=f"{env_vars.get('KAFKA_HOST')}:{env_vars.get('KAFKA_PORT')}",
        auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
        # Other configuration options...
    )   

    
    mongo_client =  MongoDBHelper(conn_host=env_vars.get('MONGO_HOST'),
                                  username = env_vars.get('MONGO_USERNAME'),
                                  password = env_vars.get('MONGO_PASSWORD'),
                                  database_name = env_vars.get('MONGO_DATABASE'))

    # Consume messages from the Kafka topic
    for message in consumer:
        message_str = message_value = message.value.decode('utf-8')
        json_message =  json.loads(message_str)
        print(json_message)
        print(f"Received message: {message_str}")
        type_message = json_message.get('Type')
        print(type_message)
        if type_message and  type_message == 'Comment':
            mongo_client.insert_one(json_message, 'Comments' )
        
        elif type_message and  type_message == 'Post':
            mongo_client.insert_one(json_message, 'Posts' )
            
except Exception as e:

    raise e
    print(e)
finally:
    # Close the Kafka consumer
    if consumer:
        consumer.close()
