from sqlalchemy import create_engine # type: ignore
from sqlalchemy.engine import URL # type: ignore


def load_to_sql(df_dict, config):
    username = config["SQLSERVER"]["username"]
    password = config["SQLSERVER"]["password"]
    server = config["SQLSERVER"]["Server"]
    database = config["SQLSERVER"]["database"]
    driver = config["SQLSERVER"]["driver"]

    connection_url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=password,
        host=server,
        database=database,
        query={"driver": driver}
    )

    engine = create_engine(connection_url)

    for table_name, df in df_dict.items():
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
        )
        print(f"Loaded table: {table_name}")
