from sqlalchemy import URL, create_engine, text
import pyodbc

from mappings import DB_CONNECTIONS

pyodbc.pooling = False

class DBConnection:

    uk_connection_strings = DB_CONNECTIONS

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
    
# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
