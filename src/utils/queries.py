check_for_schedule_existance = """SELECT s."SubredditId", ss."LastParsedDate", ss."LastCheckStatus" FROM "Reddit"."Subreddit" s
LEFT JOIN "Reddit"."SubredditSchedule" ss ON s."SubredditId" = ss."SubredditId"
WHERE s."SubrredditPath" =  '{subreddit_path}' ORDER BY s."SubredditId" desc """


insert_subreddit = """ INSERT INTO "Reddit"."Subreddit"("SubrredditPath") VALUES ('{subreddit_path}') returning "SubredditId" """

insert_subreddit_schedule = """ INSERT INTO "Reddit"."SubredditSchedule"("SubredditId", "LastParsedDate") VALUES({subreddit_id}, Null )  """

update_schedule_status_running = """ UPDATE "Reddit"."SubredditSchedule" SET  "LastCheckStatus" = 'running'  """

update_end_date = """ UPDATE "Reddit"."SubredditSchedule" SET "LastParsedDate" = NOW() , "LastCheckStatus" = '{status}'  """