from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, struct, to_json
from pyspark.sql.window import Window

query = """


SELECT 
	actorid,
	actor, 
	current_year, 
	quality_class, 
	LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year)	as previous_quality_status,
	is_active, 
	LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) as previous_active_status, 
	MIN(CURRENT_YEAR) OVER (PARTITION BY actor, actorid) as start_date, 	
	MAX(CURRENT_YEAR) OVER (PARTITION BY actor, actorid) as end_date
FROM actors
GROUP BY actor, CURRENT_YEAR, quality_class, is_active, actorid; 

"""



def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors_scd") 
    return spark.sql(query)
    

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()
    
    input_df = spark.table("actors")
    output_df = do_actor_transformation(spark, input_df)
    output_df.write.mode("overwrite").insertInto("actors_scd")