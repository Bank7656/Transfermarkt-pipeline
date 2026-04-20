import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Transfermarkt Transformations") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

gcp_project = "transfermarkt-pipeline"
bq_dataset = "transfermarkt_dwh"
spark.conf.set("temporaryGcsBucket", "football-data-storage-6532")

print("Reading raw tables from BigQuery...")
# 2. Load the Raw Tables into DataFrames
appearances_df = spark.read.format("bigquery").load(f"{gcp_project}.{bq_dataset}.appearances")
players_df = spark.read.format("bigquery").load(f"{gcp_project}.{bq_dataset}.players")
clubs_df = spark.read.format("bigquery").load(f"{gcp_project}.{bq_dataset}.clubs")
competitions_df = spark.read.format("bigquery").load(f"{gcp_project}.{bq_dataset}.competitions")
games_df = spark.read.format("bigquery").load(f"{gcp_project}.{bq_dataset}.games")

print("Transforming and Joining DataFrames...")
# 3. Perform the SQL-like Joins and Transformations
# Join appearances with players
# Join the fact table with clubs
fact_df = appearances_df.alias("a").join(
    players_df.alias("p"),
    col("a.player_id") == col("p.player_id"),
    "left"
)

fact_with_clubs = fact_df.join(
    clubs_df.alias("c"),
    col("a.player_club_id") == col("c.club_id"),
    "left"
)

fact_with_seasons = fact_with_clubs.join(
    games_df.alias("g"),
    col("a.game_id") == col("g.game_id"),
    "left"
)

# Join the result with competitions to get the league name
final_df = fact_with_seasons.join(
    competitions_df.alias("comp"),
    col("a.competition_id") == col("comp.competition_id"),
    "left"
).select(
    col("a.date").alias("match_date"),
    col("p.name").alias("player_name"),
    col("p.position"),
    col("c.name").alias("club_name"),
    col("comp.name").alias("league_name"),
    col("g.season"),
    col("a.minutes_played"),
    col("a.goals"),
    col("a.assists")
)



print("Writing transformed data back to BigQuery...")
# 4. Write the final DataFrame back to BigQuery as a new table
final_df.write \
    .format("bigquery") \
    .option("table", f"{gcp_project}.{bq_dataset}.fact_player_match_stats") \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

print("PySpark Transformation Complete!")


# final_df_with_league = final_df.join(competitions_df, on="competition_id", how="left") \
#     .select(
#         col("match_date"),
#         col("player_name"),
#         col("goals"),
#         col("assists"),
#         col("minutes_played"),
#         col("club_name"),
#         col("name").alias("league_name") # Rename 'name' so it makes sense in the dashboard
#     )