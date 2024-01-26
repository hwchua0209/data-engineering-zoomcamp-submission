import os
import argparse
from typing import Any
import pandas as pd
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.engine.base import Engine
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

# Env Variable
pg_user = os.environ.get("DB_USER")
pg_password = os.environ.get("DB_PASSWORD")

parser = argparse.ArgumentParser(description="Ingest data to SQL database")
parser.add_argument("--url", type=str, help="URL of NYD taxi data in csv.gz")
parser.add_argument("--host", type=str, help="Host of the database")
parser.add_argument("--port", type=int, help="Port of the database")
parser.add_argument("--db_name", type=str, help="Name of the database")
parser.add_argument("--table_name", type=str, help="Name of the table")
parser.add_argument("--compress", action="store_true", help="Is the file gzip")
parser.add_argument(
    "--transform_dt", action="store_true", help="Whether to transform datetime"
)


def get_file(filename: str, compress: bool = True) -> pd.DataFrame:
    if compress:
        return pd.read_csv(filename, compression="gzip")  # type: ignore
    else:
        return pd.read_csv(filename)  # type: ignore


def transform_col_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = pd.to_datetime(df[column])  # type: ignore
    return df


def postgre_db_engine(
    user: str, password: str, host: str, port: int, db_name: str
) -> Engine:
    url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    return engine


def df_to_db(
    df: pd.DataFrame, table_name: str, engine: Engine, chunksize: int
) -> None | int:
    return df.to_sql(
        name=table_name, con=engine, chunksize=chunksize, if_exists="append"
    )


def main(params: Any) -> None:
    if params.compress:
        output_filename = "output.csv.gz"
        os.system(f"wget {params.url} -O {output_filename}")
        df = get_file(output_filename)
    else:
        output_filename = "output.csv"
        os.system(f"wget {params.url} -O {output_filename}")
        df = get_file(output_filename, compress=False)

    if params.transform_dt:
        df = transform_col_to_datetime(df, "lpep_pickup_datetime")
        df = transform_col_to_datetime(df, "lpep_dropoff_datetime")

    df.columns = [colname.lower() for colname in df.columns]

    df_to_db(
        df,
        params.table_name,
        postgre_db_engine(
            pg_user, pg_password, params.host, params.port, params.db_name
        ),
        100000,
    )


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
