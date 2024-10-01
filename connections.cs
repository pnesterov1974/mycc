using Microsoft.Data.SqlClient;
using Npgsql;
using Shared;

namespace Datamech.mssql
{
    class Connections
    {
        //string pgConnectionString = "Host=localhost;Username=postgres;Password=my_pass;Database=kladr";
        //string MsSqlconnectionString = "Data Source=192.168.1.79,1433;Initial Catalog=kladr;User ID=sa;Password=Exptsci123;Trust Server Certificate=True;Connection Timeout=500";
        //string MsSqlConnectionString = "Data Source=ETL-SSIS-D-02;Initial Catalog=kladr;Integrated Security=True;Pooling=True;Trust Server Certificate=True;Connection Timeout=500";
        public Dictionary<string, string> ConnectionsDict { get; set; }
        public string GetConnectionStringByName(string connectionName)
        {// try-except
            return ConnectionsDict[connectionName];
        }

        public Connections(TargetDb tdb)
        {
            switch (tdb)
            {
                case TargetDb.mssql:
                {
                    this.ConnectionsDict = new Dictionary<string, string>()
                    {
                        {"kladrRaw", this.getMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "kladr")},
                        {"kladrWork", this.getMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "kladr")}
                    };
                    break;
                }
                case TargetDb.pgsql:
                {
                    this.ConnectionsDict = new Dictionary<string, string>()
                    {
                        {"kladrRaw", this.getPgSqlConnectionString(serverName: "localhost", dbName: "kladr")},
                        {"kladrWork", this.getPgSqlConnectionString(serverName: "localhost", dbName: "kladr")}
                    };
                    break;
                }
            }
        }
        private string getMsSqlConnectionString(string serverName, string dbName, int connectedTimeOut = 500)
        {
            SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
            {
                ApplicationName = "ETL Application",
                ConnectTimeout = connectedTimeOut,
                DataSource = serverName,
                InitialCatalog = dbName,
                IntegratedSecurity = true,
                Pooling = true,
                TrustServerCertificate = true
            };
            return sqb.ToString();
        }

        private string getPgSqlConnectionString(string serverName, string dbName)
        {
            NpgsqlConnectionStringBuilder sqb = new NpgsqlConnectionStringBuilder
            {
                ApplicationName = "ETL Application",
                Host = serverName,
                Username = "postgres",
                Password = "my_pass",
                Database = dbName

                //ApplicationName = "ETL Application",
                //Host = "localhost",
                //Username = "postgres",
                //Password = "my_pass",
                //Database = "kladr"
            };
            return sqb.ToString();
        }
    }
}
