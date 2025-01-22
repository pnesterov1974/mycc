from sqlalchemy import URL, create_engine, text, inspect
import pyodbc

pyodbc.pooling = False

class DBConnection:

    uk_connection_strings = {
        '_trd': {
            'host':'compensation-ag.am.ru',
            'database':'_trdHR_buf'
        },
        'Analytics': {
            'host':'compensation-ag.am.ru',
            'database':'Analytics'
        },
        'DWH' : {
            'host':'compensation-ag.am.ru',
            'database':'DWH'
        },
        "Pifagor2UkDev": {
            'host':'pifagor20-ag.am.ru',
            'database':'UkDev'
        }
    }

    @classmethod
    def test_connection(cls, connection_name: str, silent=False) -> bool:
        conn_str = cls.get_connection_string(connection_name)
        e = create_engine(conn_str)
        with e.connect() as connection:
            try:
                result = connection.execute(text("SELECT GETDATE() ct;"))
                for row in result:
                    if not silent:
                        print(row.ct)
                return True
            except:
                return False

    @classmethod       
    def get_sqla_engine(cls, connection_name: str):
        conn_str = cls.get_connection_string(connection_name)
        return create_engine(conn_str)

    @classmethod
    def get_connection_string(cls, connection_name: str) -> str:
        conn_params = DBConnection.uk_connection_strings[connection_name]
        host = conn_params['host']
        database = conn_params['database']
        conn_str = cls.get_sqla_connection_string_odbc(host, database)
        return conn_str

    @classmethod    
    def get_sqla_connection_string_odbc(cls, host: str, database: str) -> str:
        url_object = URL.create(
            "mssql+pyodbc",
            host=host,
            port=1433,
            database=database,
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "TrustServerCertificate": "yes",
                "authentication": "ActiveDirectoryIntegrated",
            },
        )
        return url_object
    
    @classmethod
    def inspect_db(cls, connection_name: str):
        e = cls.get_sqla_engine(connection_name)
        insp = inspect(e)
        for s in insp.get_schema_names():
            print(f"schema: {s}")
            for t in insp.get_table_names(schema=s):
                print(f"\tschema: {s} table: {t}")
                for c in insp.get_columns(table_name=t, schema=s):
                    print(f"\t\tcolumn: {c}")

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
