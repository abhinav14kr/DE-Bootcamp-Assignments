from chispa.dataframe_comparer import assert_df_equality
from src.jobs.actors_cumulative_table import do_actor_transformation
from pyspark.sql import Row

def test_actors_transformation(spark):
    # Create input test data
    input_data = [
        Row(actorid=1, films=1, year=2000, votes=100, rating=5.0, filmid=1),
        Row(actorid=1, films=1, year=2000, votes=100, rating=5.0, filmid=1),  # Duplicate
    ]

    input_dataframe = spark.createDataFrame(input_data)

    # Apply transformation
    actual_output = do_actor_transformation(spark, input_dataframe)

    # Create expected output data
    expected_output = [
        Row(
            identifier=1,
            properties='{"films":1,"year":2000,"votes":100,"rating":5.0,"filmid":1}'
        )
    ]

    expected_df = spark.createDataFrame(expected_output)

    # Assert DataFrame Equality
    assert_df_equality(actual_output, expected_df, ignore_row_order=True, ignore_column_order=True)