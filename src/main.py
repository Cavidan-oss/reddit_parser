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

#print(env_vars)
def seconds_to_human_readable(seconds):
    # Calculate hours, minutes, and remaining seconds
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Format the result
    result = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
    return result


def modify_path(user_path):
    # Ensure the path starts and ends with '/'
    user_path = user_path.strip('/')
    user_path = f'/{user_path}/'

    # Check if '/new/' is already present in the path
    if '/new/' in user_path:
        return user_path
    else:
        # Add '/new/' to the path
        return user_path + 'new/'
    

async def main(subreddit, period = None):
    scraper = RedditScraper(6)

    subreddit_path = modify_path(subreddit)

    try:

        kafka_producer = AsyncKafkaProducer(host = env_vars.get('KAFKA_HOST'), port = env_vars.get('KAFKA_PORT')) 
        postgres_helper = AsyncPostgreSQLHelper(host = env_vars.get('POSTGRES_HOST') ,port = env_vars.get('POSTGRES_PORT'), dbname=env_vars.get('POSTGRES_DATABASE'), user=env_vars.get('POSTGRES_REDDIT_USERNAME'), password=env_vars.get('POSTGRES_REDDIT_PASSWORD'))
        await postgres_helper.connect()
        
        await kafka_producer.start()

        subreddit_id, last_parsed_date = await postgres_helper.process_subreddit_conf(subreddit)
        
        if not period and last_parsed_date:
            period  =  (datetime.now() -( last_parsed_date + timedelta(hours=4)) ).total_seconds()

        if not period and not last_parsed_date:
            period = 60 * 60

        print(seconds_to_human_readable(period))

        
        async for result in scraper.parse_subreddit(subreddit_path, period=period):
            # Process each result as it becomes available

            for post_data in result:
                await kafka_producer.push_to_kafka('test1', post_data)            

    except Exception as e:
        raise e
    
    else:
        await postgres_helper.update_end_date(subreddit_id)

    finally:
        await postgres_helper.close_connection()
        await kafka_producer.stop()

if __name__ == '__main__':
    asyncio.run(main('/r/GRE') )