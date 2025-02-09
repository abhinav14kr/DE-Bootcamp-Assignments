from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, struct, to_json
from pyspark.sql.window import Window

def do_actor_transformation(spark, dataframe):
    window_spec = Window.partitionBy("actorid").orderBy("actorid")

    deduped_df = (
        dataframe.withColumn("rn", row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
    )

    transformed_df = deduped_df.select(
        dataframe.actorid.alias("identifier"),
        to_json(
            struct(
                "films", "year", "votes", "rating", "filmid"
            )
        ).alias("properties")
    )

    return transformed_df


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("players_scd") \
        .getOrCreate()
    
    input_df = spark.table("players")
    output_df = do_actor_transformation(spark, input_df)
    output_df.write.mode("overwrite").insertInto("players_scd")