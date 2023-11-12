from kafka import KafkaConsumer

# Replace these values with your Kafka bootstrap servers and topic
bootstrap_servers = 'host.docker.internal:29092'
kafka_topic = 'test'

# Configuration for PLAINTEXT
plaintext_config = {'bootstrap_servers': bootstrap_servers}

try:

    consumer = KafkaConsumer(
        kafka_topic,
        group_id='my_consumer_group',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start consuming from the beginning of the topic
        # Other configuration options...
    )

    # Consume messages from the Kafka topic
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")

finally:
    # Close the Kafka consumer
    consumer.close()
