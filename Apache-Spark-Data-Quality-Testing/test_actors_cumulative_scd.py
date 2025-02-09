from chispa.dataframe_comparer import assert_df_equality
from src.jobs.actors_scd_table import do_actor_scd_transformation
from pyspark.sql import Row, SparkSession 

def test_actor_scd_transformation():
    # Set up the Spark session
    spark = SparkSession.builder.master("local").appName("actors_scd_test").getOrCreate()

    # Create input data for the test case (based on the columns referenced in the query)
    input_data = [
        Row(actorid="nm6792464", actor="Abigail Cowen", current_year=2020, quality_class="average", previous_quality_class ='null', is_active=False, previous_active_status = 'null', start_date = 2020 , end_date = 2020),
        Row(actorid="nm0669853", actor="Aaron Pedersen", current_year=2017, quality_class="average", previous_quality_class = 'null', is_active=False, previous_active_status = 'null', start_date = 1996 , end_date = 2020),
    ]

    input_dataframe = spark.createDataFrame(input_data)

    # Create a temporary view
    input_dataframe.createOrReplaceTempView("actors")

    # Apply the transformation function
    actual_output = do_actor_scd_transformation(spark, input_dataframe)
    
    
    

    # Define expected output after applying the query logic
    expected_output = [
        Row(
            actorid="nm6792464", actor="Abigail Cowen", current_year=2020, quality_class="average", previous_quality_class ='null', is_active=False, previous_active_status = 'null', start_date = 2020 , end_date = 2020
        ),
        Row(
           actorid="nm0669853", actor="Aaron Pedersen", current_year=2017, quality_class="average", previous_quality_class = 'null', is_active=False, previous_active_status = 'null', start_date = 1996 , end_date = 2020),
    ]

    expected_df = spark.createDataFrame(expected_output)

    # Assert that the output from the transformation matches the expected output
    assert_df_equality(actual_output, expected_df, ignore_row_order=True, ignore_column_order=True)
