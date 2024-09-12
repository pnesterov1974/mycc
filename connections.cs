using Microsoft.Data.SqlClient;

namespace Datamech;

public static class ConnectionStrings
{
    public static string etlMsSqlSourceConnectionString
    {
        get
        {
            SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
            {
                ApplicationName = "GEO DataPipe Application",
                ConnectTimeout = 500,
                DataSource = "pythia-ag.am.ru",
                InitialCatalog = "Pythoness",
                IntegratedSecurity = true,
                Pooling = true,
                TrustServerCertificate = true
            };
            return sqb.ToString();
        }
    }

    public static string etlMsSqlTargetConnectionString
    {
        get
        {
            SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
            {
                ApplicationName = "GEO DataPipe Application",
                ConnectTimeout = 500,
                DataSource = "etl-ssis-d-02",
                InitialCatalog = "etln",
                IntegratedSecurity = true,
                Pooling = true,
                TrustServerCertificate = true
            };
            return sqb.ToString();
        }
    }
}
