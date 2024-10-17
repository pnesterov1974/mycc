import pyodbc
from sqlalchemy.engine import URL, create_engine
from sqlalchemy import text

servers = {
    "kladr": {
        "driver" : '{ODBC Driver 17 for SQL Server}',
        "server_name" : 'ETL-SSIS-D-02',
        "db_name" : 'kladr',
        "app_name" : 'ETL Application'
        },
    "dwh": {
        "driver" : '{ODBC Driver 17 for SQL Server}',
        "server_name" : 'comp-db-p-02',
        "db_name" : 'dwh',
        "app_name" : 'ETL Application'
        },
    }

class Conns:

    def __init__(self, db_name: str) -> None:
        self.dbinfo = servers[db_name]

    def get_conn_str(self) -> str:
        srv = self.dbinfo
        conn_str = f'DRIVER={srv["driver"]};SERVER={srv["server_name"]};DATABASE={srv["db_name"]};Trusted_Connection=yes;APP={srv["app_name"]};'
        return conn_str
    
    def get_odbc_conn(self):
        return pyodbc.connect(self.get_conn_str())
    
    def get_sqla_engine(self):
        conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": self.get_conn_str()})
        return create_engine(conn_url)


class ConnTest(Conns):

    def __init__(self, db_name: str, silent=True) -> None:
        super().__init__(db_name)
        self.silent = silent
    
    def test_sqla_conn(self) -> bool:
        e = self.get_sqla_engine()
        try:
            conn = e.connect()
            sql = text("SELECT GET_DATE();")
            results = conn.execute(sql)
            if not self.silent:
                for records in results:
                    print(records)
            conn.close()
            return True
        except Exception as ex:
            print(str(ex))
            return False

    def test_odbc_conn(self) -> bool:
        try:
            c = self.get_odbc_conn()
            sql = "SELECT GET_DATE();"
            rows = c.cursor().execute(sql).fetchall()
            if not self.silent:
                for row in rows:
                    print(row)
            c.close()
            return True
        except Exception as ex:
            print(str(ex))
            return False


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    pass
