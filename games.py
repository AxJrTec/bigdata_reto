from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("games")\
        .getOrCreate()

    print("leyendo dataset.csv ... ")
    path_games="dataset.csv"
    df_games = spark.read.csv(path_games, header=True, inferSchema=True)
    df_games.createOrReplaceTempView("games")

    query='SELECT name, metacritic, released, added_status_owned AS owned, added_status_playing AS playing FROM games WHERE metacritic is NOT NULL AND metacritic > 70 ORDER BY metacritic DESC'
    
    df_games_filtered = spark.sql(query)
    df_games_filtered.show(20)
    results = df_games_filtered.toJSON().collect()
    df_games.write.mode("overwrite").json("results")

    with open('results/data.json', 'w') as file:
        json.dump(results, file)
