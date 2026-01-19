from pyspark.sql import DataFrame

def validate_non_empty(df: DataFrame) -> None:
    cnt = df.count()
    if cnt == 0:
        raise ValueError("Validation failed: Dataframe is empty.")
    print(f"Validation passed: row_count = {cnt}")

def validate_required_columns(df:DataFrame, required_cols: list[str]) -> None:
    cols = set(df.columns)
    missing = [c for c in required_cols if c not in cols]
    if missing:
        raise ValueError(f"Validation failed:missing columns: {missing}")
    print(f"Validation passed: required columns present: {required_cols}")