using Npgsql;
using static Shared.MyLogger;

namespace Utils;

public class UtilsPgSql: IDBUtils
{
    public bool ClearDestTable(string connectionString, string targetTableFullName)
    {
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
        var dataSource = dataSourceBuilder.Build();
        string truncateTableSql = $"TRUNCATE TABLE {targetTableFullName};";
        try
        {
            using NpgsqlConnection conn = dataSource.OpenConnection();
            NpgsqlCommand cmd = new NpgsqlCommand(truncateTableSql, conn);
            Log.Information("Очистка таблицы {table}", targetTableFullName);
            Log.Debug("SQL:{sql}", cmd.CommandText);
            cmd.ExecuteNonQuery();
            Log.Information("Таблица {table} очищена...", targetTableFullName);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка при очистке таблицы: \n {ErrMessage}", ex.Message);
            return false;
        }
    }

    public bool TryDbConnection(string connectionString)
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
}
