import asyncpg
import asyncio
from datetime import datetime
from dotenv import dotenv_values

env_vars = dotenv_values(".env")

check_for_schedule_existance = """SELECT s."SubredditId", ss."LastParsedDate", ss."LastCheckStatus" FROM "Reddit"."Subreddit" s
LEFT JOIN "Reddit"."SubredditSchedule" ss ON s."SubredditId" = ss."SubredditId"
WHERE s."SubredditPath" =  '{subreddit_path}' ORDER BY s."SubredditId" desc """


insert_subreddit = """ INSERT INTO "Reddit"."Subreddit"("SubredditPath") VALUES ('{subreddit_path}') returning "SubredditId" """

insert_subreddit_schedule = """ INSERT INTO "Reddit"."SubredditSchedule"("SubredditId", "LastParsedDate") VALUES({subreddit_id}, Null )  """

update_schedule_status_running = """ UPDATE "Reddit"."SubredditSchedule" SET  "LastCheckStatus" = 'running' WHERE "SubredditId" = {subreddit_id} """

update_end_date_query = """ UPDATE "Reddit"."SubredditSchedule" SET "LastParsedDate" = NOW() , "LastCheckStatus" = '{status}' WHERE "SubredditId" = {subreddit_id} """


class AsyncPostgreSQLHelper:
    
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.dbname = dbname if dbname else ''
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    async def connect(self):

        conn_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

        self.connection = await asyncpg.connect(
            conn_string
        )

    async def process_subreddit_conf(self,subreddit_path):

        query = check_for_schedule_existance.format(subreddit_path = subreddit_path)
        result = await self.fetch_one(query)
        sub_id, sub_parsed_date, last_status = result.get('SubredditId') if result else None, result.get('LastParsedDate')  if result else None , result.get('LastCheckStatus')  if result else None
        
        schedule_status = await self.check_for_schedule(sub_id, sub_parsed_date)
        
        if schedule_status:
            return (sub_id, sub_parsed_date)

        if not sub_id:
            sub_id = await self.insert_subreddit(subreddit_path)
            
        if not sub_parsed_date:
            await self.insert_schedule(sub_id)

            return (sub_id, None)
        
    

    async def update_end_date(self,subreddit_id):

        try:
            await self.execute_query(update_end_date_query.format(status = 'ended', subreddit_id = subreddit_id))

            return True

        except Exception as e:
            raise e
        

    async def insert_subreddit(self,subreddit_path):
        try:
            inserted = await self.fetch_one(insert_subreddit.format(subreddit_path = subreddit_path))


            return inserted.get('SubredditId') if inserted else None
        
        except Exception as e:
            print("Fail to Insert subreddit. Aborting ...")
            raise Exception

    async def insert_schedule(self, subreddit_id):
        try:
            await self.execute_query(insert_subreddit_schedule.format(subreddit_id = subreddit_id))

        except Exception as e:

            print("Fail to insert a schedule. Aborting ...")
            raise Exception


    async def check_for_schedule(self, sub_id, sub_parsed_date):

        if sub_id and sub_parsed_date:
            return True
        
        return False



    async def execute_query(self, query, *params):
        try:
            
            await self.connection.execute(query, *params)
            print("Query executed successfully")
        except Exception as e:
            print(f"Error executing query: {e}")
            

    async def fetch_data(self, query, *params):
        try:
            async with self.connection.transaction():
                async for row in self.connection.cursor(query, *params):
                    yield row

        except Exception as e:
            print(f"Error fetching data: {e}")

    async def fetch_one(self, query, *params):
        try:
            async with self.connection.transaction():
                row = await self.connection.fetchrow(query, *params)
                return row
        except Exception as e:
            print(f"Error fetching one row: {e}")

    async def close_connection(self):
        await self.connection.close()
        print("Postgres Connection closed")



# Example usage:
# Replace 'your_db', 'your_user', and 'your_password' with your actual database credentials
async def main():

    db_helper = AsyncPostgreSQLHelper(host = env_vars.get('POSTGRES_HOST') ,port = env_vars.get('POSTGRES_PORT'), dbname=env_vars.get('POSTGRES_DATABASE'), user=env_vars.get('POSTGRES_REDDIT_USERNAME'), password=env_vars.get('POSTGRES_REDDIT_PASSWORD'))

    await db_helper.connect()

    
    # query = """ INSERT INTO "Reddit"."Subreddit"("SubrredditPath")  VALUES('/test') returning "SubredditId" """
    # test = await db_helper.fetch_one(query)
    # print(test.get("SubredditId"))

    res =await db_helper.process_subreddit_conf('/r/Test')

    print(res)




    # select_data_query = """ SELECT * FROM "Reddit"."Subreddit"; """

    # res = await db_helper.fetch_one(select_data_query)

    # print(res.get('SubredditId'))

    # Fetch data using iterator
    # async for row in db_helper.fetch_data(select_data_query):
    #     print(row.keys())
    #     # for i , j in row.items():
    #     #     print(i,j)

    #     # print(row.keys())
    #     # for i  in row.keys():
    #     #     print(i)


    #     print(row.values())

    #     for i  in row.values():
    #         print(i)

    #     print(row.get('SubrredditPath'))    


    # Fetch one row
    # one_row = await db_helper.fetch_one(select_data_query)
    # print("One row from the table:")
    # print(one_row)

    await db_helper.close_connection()

# Run the event loop
if __name__ == "__main__":
    asyncio.run(main())

