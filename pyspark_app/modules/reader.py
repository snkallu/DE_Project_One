from pyspark.sql import SparkSession, DataFrame

def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Read a Databricks table (Unity Catalog or Hive metastore) into a DataFrame.
    Example table_name:
      - "game_pass_games"
      - "default.game_pass_games"
      - "hive_metastore.default.game_pass_games"
      - "main.analytics.game_pass_games"
    """
    return spark.table(table_name)