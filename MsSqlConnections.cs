public static class MsSqlConnections
{
    public static string MsSqlConnectionString(string serverName, string dbname)
    {
        SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
        {
            ApplicationName = "ETL Application",
            ConnectTimeout = 50,
            CommandTimeout = 30,
            DataSource = serverName,
            InitialCatalog = dbname,
            IntegratedSecurity = true,
            Pooling = true,
            TrustServerCertificate = true,
            MultipleActiveResultSets = true
        };
        return sqb.ToString();
    }

    public static bool TestMsSqlConnectionString(string connString)
    {
        //
        return true;
    }
}
