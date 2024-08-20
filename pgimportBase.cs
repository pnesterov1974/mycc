using Npgsql;
using Serilog;
using DbfDataReader;
using Shared;

namespace KladrData.DbfToPg;

public class PgImportBase
{
    public PgImportBase(ObjectInfo oi) => this.objectInfo = oi;

    public ObjectInfo objectInfo { get; set; }

    public ILogger Log { get; set; }

    protected DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    private string TruncateTebleSql
    {
        get => $"TRUNCATE TABLE {this.objectInfo.DestinationTableFullName};";
    }

    protected bool clearDestTable()
    {
        var dataSourceBuilder = new NpgsqlDataSourceBuilder(this.objectInfo.ConnectionString);
        var dataSource = dataSourceBuilder.Build();

        try
        {
            using NpgsqlConnection conn = dataSource.OpenConnection();
            NpgsqlCommand cmd = new NpgsqlCommand(this.TruncateTebleSql, conn);
            Log.Information("Очистка таблицы {table}", this.objectInfo.DestinationTableName);
            Log.Debug("SQL:{sql}\n", this.TruncateTebleSql);
            cmd.ExecuteNonQuery();
            Log.Information("Таблица {table} очищена...", this.objectInfo.DestinationTableName);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error("Ошибка при очистке таблицы: \n {ErrMessage}", ex.Message);
            return false;
        }
    }
}
