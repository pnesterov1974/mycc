using System.Text;
using Microsoft.Data.SqlClient;
using DbfDataReader;
using Npgsql;
using Shared;
using Serilog;
using System.Data;

namespace KladrData;

public class DBUtils
{
    public static ILogger Log;
    public static bool TryMsSqlDbConnection(string connectionString)
    {
        Log.Information("Connection String: {@connString}", connectionString);
        try
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                string connInfo = GetMsSqlConnectionInformation(conn);
                Log.Debug("Информация о соединении:\n {conn}", connInfo);
                using (SqlCommand cmd = new SqlCommand("SELECT GETDATE();", conn))
                {
                    cmd.CommandType = CommandType.Text;
                    var dt = cmd.ExecuteScalar();
                    Log.Information("Подключено успешно...{dt}", dt);
                    return true;
                }
            }
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка проверки подключения с Базе Данных:\n SQL:{message}", ex.Message);
            return false;
        }
        // may be return from here
    }

    public static bool TryPgDbConnection(string connectionString)
    {
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
        var dataSource = dataSourceBuilder.Build();

        try
        {
            NpgsqlConnection conn = dataSource.OpenConnection();
            NpgsqlCommand cmd = new NpgsqlCommand("SELECT NOW();", conn);
            var dt = cmd.ExecuteScalar();
            Log.Information("Подключено успешно...{dt}", dt);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка проверки подключения с Базе Данных:\n SQL:{message}", ex.Message);
            return false;
        }
    }

    public static bool ReadDbfInfo(ObjectInfo oi)
    {
        Log.Information("Считывание информации о файле {filepath}", oi.SourceFilePath);
        long recordCount = 0;
        if (File.Exists(oi.SourceFilePath))
        {
            Log.Information("Файл {filepath} найден, обработка...", oi.SourceFilePath);
            using (var dbfTable = new DbfTable(oi.SourceFilePath, Encoding.UTF8))
            {
                var header = dbfTable.Header;
                var versionDescription = header.VersionDescription;
                var hasMemo = dbfTable.Memo != null;
                recordCount = header.RecordCount;
                Log.Information("versionDescription: {versionDescription}\thasMemo: {hasMemo}\trecordCount: {recordCount}", versionDescription, hasMemo, recordCount);
                foreach (var dbfColumn in dbfTable.Columns)
                {
                    var name = dbfColumn.ColumnName;
                    var columnType = dbfColumn.ColumnType;
                    var length = dbfColumn.Length;
                    var decimalCount = dbfColumn.DecimalCount;
                    Log.Information("name: {name}\tcolumnType: {columnType}\tlength: {length}\tdecimalCount: {decimalCount}", name, columnType, length, decimalCount);
                }
            }
            return recordCount > 0;
        }
        else
        {
            Log.Information("Файла {@filepath} не сушествует, останов...", oi.SourceFilePath);
            return false;
        }
    }

    public static string GetMsSqlConnectionInformation(SqlConnection cnn)
    {
        StringBuilder sb = new StringBuilder(1024);

        sb.AppendLine("Connection String: " + cnn.ConnectionString);
        sb.AppendLine("State: " + cnn.State.ToString());
        sb.AppendLine("Connection Timeout: " + cnn.ConnectionTimeout.ToString());
        sb.AppendLine("Database: " + cnn.Database);
        sb.AppendLine("Data Source: " + cnn.DataSource);
        sb.AppendLine("Server Version: " + cnn.ServerVersion);
        sb.AppendLine("Workstation ID: " + cnn.WorkstationId);

        return sb.ToString();
    }
}
