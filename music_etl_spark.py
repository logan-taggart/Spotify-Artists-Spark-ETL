from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count, round

# ------------------
# EXTRACT
# ------------------

spark = SparkSession.builder.appName("Spotify ETL").getOrCreate()

df = spark.read.option("header", True).csv("music_data/SpotifyFeatures.csv")

# ------------------
# TRANSFORM
# ------------------

# Filter for genre == "Rap" and cast popularity to int
tracks = df.filter(col("genre") == "Rap")
tracks = tracks.withColumn("popularity", col("popularity").cast("int"))

# Group and compute artist stats
artist_stats = tracks.groupBy("artist_name") \
    .agg(
        round(avg("popularity"), 2).alias("avg_popularity"),
        round(stddev("popularity"), 2).alias("popularity_stddev"),
        count("*").alias("num_tracks")
    )

# Filter and sort
consistent_rap_artists = artist_stats \
    .filter((col("num_tracks") >= 100) & (col("popularity_stddev") <= 10)) \
    .orderBy(col("avg_popularity").desc(), col("popularity_stddev").asc())

# ------------------
# LOAD
# ------------------

# Show and save results
consistent_rap_artists.show(5, truncate=False)
consistent_rap_artists.write.csv("results", header=True, mode="overwrite")

spark.stop()
