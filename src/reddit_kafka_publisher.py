import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

from scrapers.reddit_parser import RedditScraper
from utils.kafka_producer import AsyncKafkaProducer
from utils.postgres_helper import AsyncPostgreSQLHelper
import asyncio
from dotenv import dotenv_values
from datetime import datetime, timedelta



env_vars = dotenv_values(".env")


class RedditKafkaProducer(RedditScraper):

    def __init__(self) -> None:
        super().__init__()
        self.event_loop = asyncio.get_event_loop()

        self.kafka_producer = None
        self.postgres_helper = None

        self._kafka_connection_status = False
        self._postgres_connection_status = False
        

    def get_modified_subreddit_path(self,subreddit_path):
        user_path = subreddit_path.strip('/')
        user_path = f'/{user_path}/'

        # Check if '/new/' is already present in the path
        if '/new/' in user_path:
            return user_path
        else:
            # Add '/new/' to the path
            return user_path + 'new/'

    
    @staticmethod
    def get_human_readable_time(seconds):
        # Calculate hours, minutes, and remaining seconds
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Format the result
        result = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        return result
            
    async def subreddit_kafka_producer(self, subreddit, kafka_topic = 'test', period = None):

        try:
            self.kafka_producer = await self.get_kafka_connection() 
            self.postgres_helper = await self.get_postgres_connection()

            subreddit_id, last_parsed_date = await self.postgres_helper.process_subreddit_conf(subreddit)
            
            
            if not period and last_parsed_date:
                period  =  (datetime.now() -( last_parsed_date + timedelta(hours=4)) ).total_seconds()

            if not period and not last_parsed_date:
                period = 60 * 60

            print(RedditKafkaProducer.get_human_readable_time(period))

            subreddit_path = self.get_modified_subreddit_path(subreddit)

            await self.parse_and_publish(subreddit_path, kafka_topic, period)

        except Exception as e:
            print("Error Occured")

            print(e)


        finally:
            if self.kafka_producer:
                await self.kafka_producer.stop()
            if self.postgres_helper:
                await self.postgres_helper.close_connection()

    async def parse_and_publish(self, subreddit_path, kafka_topic, period):

        async for result in self.parse_subreddit(subreddit_path, period=period):
            # Process each result as it becomes available
            for post_data in result:
                post_data.update({'SubredditName': subreddit_path})
                await self.kafka_producer.push_to_kafka(kafka_topic, post_data)     


    async def get_kafka_connection(self):
        try:
            if not self._kafka_connection_status:
                self.async_kafka_producer = AsyncKafkaProducer(
                    host=env_vars.get('KAFKA_HOST'), port=env_vars.get('KAFKA_PORT')
                )
                await self.async_kafka_producer.start()
                self._kafka_connection_status = True

            return self.async_kafka_producer
        except Exception as e:
            print("Failed to Connect Apache Kafka")
            raise e

    async def get_postgres_connection(self):
        try:
            if not self._postgres_connection_status:
                self.async_postgres_helper = AsyncPostgreSQLHelper(
                    host=env_vars.get('POSTGRES_HOST'),
                    port=env_vars.get('POSTGRES_PORT'),
                    dbname=env_vars.get('POSTGRES_DATABASE'),
                    user=env_vars.get('POSTGRES_REDDIT_USERNAME'),
                    password=env_vars.get('POSTGRES_REDDIT_PASSWORD')
                )
                await self.async_postgres_helper.connect()
                self._postgres_connection_status = True

            return self.async_postgres_helper
        
        except Exception as e:
            print("Failed to Connect to PostgreSQL")
            raise e


if __name__ == '__main__':

    producer =  RedditKafkaProducer()

    asyncio.run(producer.subreddit_kafka_producer('r/GRE/', period=60 * 60* 12))