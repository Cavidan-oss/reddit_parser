import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")

from scrapers.reddit_parser import RedditScraperAsyncIterator
from utils.kafka_producer import AsyncKafkaProducer
import asyncio


if __name__ == '__main__':
    async def main():
        async for result in RedditScraperAsyncIterator(max_current_tabs=6):
            print(result)

    asyncio.run(main())