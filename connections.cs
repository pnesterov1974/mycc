using Microsoft.Data.SqlClient;
using Npgsql;
using Shared;

namespace Datamech
{
    public static class Connections
    {
        //string pgConnectionString = "Host=localhost;Username=postgres;Password=my_pass;Database=kladr";
        //string MsSqlconnectionString = "Data Source=192.168.1.79,1433;Initial Catalog=kladr;User ID=sa;Password=Exptsci123;Trust Server Certificate=True;Connection Timeout=500";
        //string MsSqlConnectionString = "Data Source=ETL-SSIS-D-02;Initial Catalog=kladr;Integrated Security=True;Pooling=True;Trust Server Certificate=True;Connection Timeout=500";
        public static Dictionary<string, string> ConnectionsDict { get; set; } = new Dictionary<string, string>();
        public static void InitConnections(TargetDb tdb)
        {
            switch (tdb)
            {
                case TargetDb.mssql:
                    {
                        ConnectionsDict.Add("kladrRaw", Connections.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "kladr"));
                        ConnectionsDict.Add("kladrWork", Connections.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "kladr"));
                        ConnectionsDict.Add("dwh", Connections.buildMsSqlConnectionString(serverName: "comp-db-p-02", dbName: "dwh"));
                        ConnectionsDict.Add("etlSsisP02", Connections.buildMsSqlConnectionString(serverName: "ETL-SSIS-P-02", dbName: "ssisdbext"));
                        break;
                    }
                case TargetDb.pgsql:
                    {
                        ConnectionsDict.Add("kladrRaw", Connections.buildPgSqlConnectionString(serverName: "localhost", dbName: "kladr"));
                        ConnectionsDict.Add("kladrWork", Connections.buildPgSqlConnectionString(serverName: "localhost", dbName: "kladr"));
                        break;
                    }
            }
        }

        public static string GetConnectionStringByName(string connectionName)
        {
            try
            {
                return Connections.ConnectionsDict[connectionName];
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Неверное имя подклюсения к БД: {connectionName}", ex);
            }
        }

        private static string buildMsSqlConnectionString(string serverName, string dbName, int connectedTimeOut = 500)
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

        private static string buildPgSqlConnectionString(string serverName, string dbName)
        {
            NpgsqlConnectionStringBuilder sqb = new NpgsqlConnectionStringBuilder
            {
                ApplicationName = "ETL Application",
                Host = serverName,
                Username = "postgres",
                Password = "my_pass",
                Database = dbName,
                Timeout = 100,
                CommandTimeout = 0,
                Pooling = true
            };
            return sqb.ToString();
        }

        public static bool ConnectToTheSameMsSqlInstance(string conn1, string conn2, bool CompareServerOnly = true)
        {
            SqlConnectionStringBuilder sqb1 = new SqlConnectionStringBuilder(conn1);
            SqlConnectionStringBuilder sqb2 = new SqlConnectionStringBuilder(conn2);

            if (CompareServerOnly)
            {
                try
                {
                    return string.Equals(sqb1.DataSource, sqb2.DataSource);
                }
                catch
                {
                    return false;
                }
            }
            else
            {
                try
                {
                    return string.Equals(sqb1.DataSource, sqb2.DataSource) && string.Equals(sqb1.InitialCatalog, sqb2.InitialCatalog);
                }
                catch
                {
                    return false;
                }
            }
        }

        public static bool ConnectToTheSamePgSqlInstance(string conn1, string conn2, bool CompareServerOnly = true)
        {
            NpgsqlConnectionStringBuilder sqb1 = new NpgsqlConnectionStringBuilder(conn1);
            NpgsqlConnectionStringBuilder sqb2 = new NpgsqlConnectionStringBuilder(conn2);

            if (CompareServerOnly)
            {
                try
                {
                    return string.Equals(sqb1.Host, sqb2.Host);
                }
                catch
                {
                    return false;
                }
            }
            else
            {
                try
                {
                    return (string.Equals(sqb1.Host, sqb2.Host)) && (string.Equals(sqb1.Database, sqb2.Database));
                }
                catch
                {
                    return false;
                }
            }
        }
    }
}
