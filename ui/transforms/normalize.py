import pandas as pd


def rebase_to_1(df: pd.DataFrame) -> pd.DataFrame:
    base = df.iloc[0]
    return df / base
