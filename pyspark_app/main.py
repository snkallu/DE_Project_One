from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim 

from pyspark_app.modules.reader import read_table
from pyspark_app.modules.validator import validate_non_empty
from pyspark_app.modules.writer import write_delta_table


def main():
    spark = SparkSession.builder.appName("DE_Project_One_GamePass").getOrCreate()

    source_table = "game_pass_games"
    target_table = "game_pass_games_silver"

    print(f"Reading source table :{source_table}")
    df = read_table(spark, source_table)

    validate_non_empty(df)

    print(f"Preview of columns:", df.columns)

 
    # 1) trim string columns (we’ll apply trim defensively by trying it on all columns)
    cleaned = df
    for c in df.columns:
        dtype = dict(df.dtypes).get(c)
        if dtype == "string":
            cleaned = cleaned.withColumn(c, trim(col(c))).withColumnRenamed(c, c.replace(' ', '_').replace('%', '_'))
    # Drop duplicate rows
    cleaned = cleaned.dropDuplicates()

    # Fix invalid column names for Delta('COMP %' -> 'comp_pct')
    rename_map = {
        "COMP %" : "comp_pct",
    }
    for old, new in rename_map.items():
        if old in cleaned.columns:
            cleaned = cleaned.withColumnRenamed(old, new)

    print("🔹 Final columns after rename:", cleaned.columns)

    print(f"Writing target table :{target_table}")
    write_delta_table(cleaned, target_table, mode = "overwrite")
    
    print("Day 1 pipeline completed successfully.")
    
if __name__ == "__main__":
    main()
