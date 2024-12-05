using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;

namespace Utils;

public class UtilsMsSql: IDBUtils
{
    public bool ClearDestTable(string connectionString, string targetTableFullName)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            try
            {
                conn.Open();
                string truncateTableSql = $"TRUNCATE TABLE {targetTableFullName};";
                SqlCommand cmd = new SqlCommand(truncateTableSql, conn);
                cmd.ExecuteNonQuery();
                Log.Debug("Очистка таблицы {table} \nSQL:{sql}", targetTableFullName, cmd.CommandText);
                Log.Information("Таблица {table} очищена...", targetTableFullName);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error("Ошибка при очистке таблицы {table}: \n {ErrMessage}", targetTableFullName, ex.Message);
                return false;
            }
        }
    }

    public bool TryDbConnection(string connectionString) // MsSqlConnection
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


    public string GetMsSqlConnectionInformation(SqlConnection cnn)
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
