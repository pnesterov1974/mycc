using Microsoft.Data.SqlClient;
using Npgsql;

namespace Shared.connections
{
    public static class ConnectionStrings
    {
        public static Dictionary<string, string> ConnectionsDict { get; set; } = new Dictionary<string, string>();

        public static bool ConnectionStringsInitialized()
        {
            return ConnectionsDict.Count > 0;
        }

        public static void InitConnectionStrings(TargetDb tdb, bool useAD = false)
        {
            switch (tdb)
            {
                case TargetDb.mssql:
                    {
                        ConnectionsDict.Add("kladrStage", ConnectionStrings.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "kladrStg", useAD:useAD));
                        ConnectionsDict.Add("garStage", ConnectionStrings.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "garStg", useAD:useAD));
                        //ConnectionsDict.Add("kladrStage", ConnectionStrings.buildMsSqlConnectionString(serverName: "192.168.1.81,1433", dbName: "kladr", useAD:useAD));
                        ConnectionsDict.Add("kladrWork", ConnectionStrings.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "etln", useAD:useAD));
                        ConnectionsDict.Add("ndwh", ConnectionStrings.buildMsSqlConnectionString(serverName: "comp-db-p-02", dbName: "dwh", useAD:useAD));
                        ConnectionsDict.Add("ssisp01", ConnectionStrings.buildMsSqlConnectionString(serverName: "ETL-SSIS-P-01", dbName: "SSISDB", useAD:useAD));
                        ConnectionsDict.Add("myDWH", ConnectionStrings.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "mydwh", useAD:useAD));
                        ConnectionsDict.Add("techDb", ConnectionStrings.buildMsSqlConnectionString(serverName: "ETL-SSIS-D-02", dbName: "techdb", useAD:useAD));
                        ConnectionsDict.Add("trdBuffer", ConnectionStrings.buildMsSqlConnectionString(serverName: "comp-db-p-02", dbName: "_trdHR_buf", useAD:useAD));
                        break;
                    }
                case TargetDb.pgsql:
                    {
                        ConnectionsDict.Add("kladr", ConnectionStrings.buildPgSqlConnectionString(serverName: "127.0.0.1:5432", dbName: "kladr"));
                        ConnectionsDict.Add("gar", ConnectionStrings.buildPgSqlConnectionString(serverName: "127.0.0.1:5432", dbName: "gar"));
                        ConnectionsDict.Add("kladrStage", ConnectionStrings.buildPgSqlConnectionString(serverName: "127.0.0.1:5432", dbName: "kladrStage"));
                        //ConnectionsDict.Add("kladrWork", ConnectionStrings.buildPgSqlConnectionString(serverName: "127.0.0.1:5432", dbName: "workDB"));
                        ConnectionsDict.Add("kladrWork", ConnectionStrings.buildPgSqlConnectionString(serverName: "127.0.0.1:5432", dbName: "kladrStage"));
                        break;
                    }
            }
        }

        public static string GetConnectionStringByName(string connectionName)
        {
            try
            {
                return ConnectionStrings.ConnectionsDict[connectionName];
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Неверное имя подклюсения к БД: {connectionName}", ex);
            }
        }

        private static string buildMsSqlConnectionString(string serverName, string dbName, int connectedTimeOut = 500, bool useAD = false)
        {
            if (useAD)
            {
                SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
                {
                    ApplicationName = "ETL Application",
                    ConnectTimeout = connectedTimeOut,
                    CommandTimeout = 30,
                    DataSource = serverName,
                    InitialCatalog = dbName,
                    IntegratedSecurity = true,
                    Pooling = true,
                    TrustServerCertificate = true,
                    MultipleActiveResultSets = true
                };
                return sqb.ToString();
            }
            else
            {
                SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
                {
                    ApplicationName = "ETL Application",
                    ConnectTimeout = connectedTimeOut,
                    CommandTimeout = 30,
                    DataSource = serverName,
                    InitialCatalog = dbName,
                    UserID = "sa",
                    Password = "Extlvo123",
                    //IntegratedSecurity = true,
                    Pooling = true,
                    TrustServerCertificate = true,
                    MultipleActiveResultSets = true
                };
                return sqb.ToString();
            }
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
                Pooling = true,
                IncludeErrorDetail = true
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
