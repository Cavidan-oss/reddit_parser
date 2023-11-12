import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

from scrapers.reddit_parser import RedditScraper
from utils.kafka_producer import AsyncKafkaProducer
import asyncio
from dotenv import dotenv_values

env_vars = dotenv_values(".env")

#print(env_vars)



async def main(subreddit):
    scraper = RedditScraper(6)
    kafka_producer = AsyncKafkaProducer(f"{env_vars.get('KAFKA_HOST')}:{env_vars.get('KAFKA_PORT')}")
    await kafka_producer.start()

    async for result in scraper.parse_subreddit(subreddit):
        # Process each result as it becomes available

        for post_data in result:
            await kafka_producer.push_to_kafka('test1', post_data)            
            await asyncio.sleep(0.2)

        
    await kafka_producer.stop()

if __name__ == '__main__':


    asyncio.run(main('/r/GRE/new/'))