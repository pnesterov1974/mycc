using System.Text;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace Shared;

public enum GeoType
{
    kladr,
    gar
}

public enum TargetDb
{
    mssql,
    pgsql
}

public enum TargetTable
{
    SocrBase,
    AltNames,
    Kladr,
    Streets,
    Doma,
    NameMap
}

public struct ObjectInfo
{
    public string DestinationTableName;
    public string DestinationSchemaName;
    public int BufferRecs;
    public string SourceFileName;
    public string SourceDirPath;
    public string ConnectionString;

    public readonly string DestinationTableFullName
    {
        get => string.Join('.', this.DestinationSchemaName, this.DestinationTableName);
    }

    public readonly string SourceFilePath
    {
        get => Path.Join(SourceDirPath, SourceFileName);
    }

    public override string ToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"DestinationSchemaName:\t{this.DestinationSchemaName}\n");
        sb.Append($"DestinationTableName:\t{this.DestinationTableName}\n");
        sb.Append($"BufferRecs:\t{this.BufferRecs}\n");
        sb.Append($"SourceFileName:\t{this.SourceFileName}\n");
        sb.Append($"SourceDirPath:\t{this.SourceDirPath}\n");
        sb.Append($"ConnectionString:\t{this.ConnectionString}\n");
        return sb.ToString();
    }
}

public enum DBSource
{
    mssql,
    pgsql
}

public static class ConnectionString
{
    //string pgConnectionString = "Host=localhost;Username=postgres;Password=my_pass;Database=kladr";
    //string MsSqlconnectionString = "Data Source=192.168.1.79,1433;Initial Catalog=kladr;User ID=sa;Password=Exptsci123;Trust Server Certificate=True;Connection Timeout=500";
    //string MsSqlConnectionString = "Data Source=ETL-SSIS-D-02;Initial Catalog=kladr;Integrated Security=True;Pooling=True;Trust Server Certificate=True;Connection Timeout=500";
    public static string GetConnectionString(DBSource dbs = DBSource.mssql)
    {
        switch (dbs)
        {
            case DBSource.mssql:
            {
                return getMsSqlConnectionString();
            }
            case DBSource.pgsql:
            {
                 return getPgSqlConnectionString();
            }
            default:
            {
                return getMsSqlConnectionString();
            }
        }
    } 
    private static string getMsSqlConnectionString()
    {
        SqlConnectionStringBuilder sqb = new SqlConnectionStringBuilder
        {
            ApplicationName = "GEO DataPipe Application",
            ConnectTimeout = 500,
            DataSource = "ETL-SSIS-D-02",
            InitialCatalog = "kladr",
            IntegratedSecurity = true,
            Pooling = true,
            TrustServerCertificate = true
        };
        return sqb.ToString();
    }

    private static string getPgSqlConnectionString()
    { 
        NpgsqlConnectionStringBuilder sqb = new NpgsqlConnectionStringBuilder
        {
            ApplicationName = "GEO DataPipe Application",
            Host = "localhost",
            Username = "postgres",
            Password = "my_pass",
            Database = "kladr"
        };
        return sqb.ToString();
    }
}
