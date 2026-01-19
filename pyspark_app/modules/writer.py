from pyspark.sql import DataFrame

def write_delta_table(df:DataFrame, full_table_name: str, mode: str = "overwrite") -> None:
    """
    Writes a DataFrame a a managed Delta table.
    full_table_name examples:
    - "game_pass_games_silver"
    - default.gane_pass_games_silver"
    - "hive_metastore.default.game_pass_games_silver"
    
    """
    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(full_table_name)
    )
    print(f"Wrote Delta table: {full_table_name} (mode={mode})")