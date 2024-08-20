using Microsoft.Data.SqlClient;
using Serilog;
using DbfDataReader;
using Shared;

namespace KladrData.DbfToMsSQL;

public class MsSqlImportDbfCommon
{
    public MsSqlImportDbfCommon(ObjectInfo oi) => this.objectInfo = oi;
    
    private ObjectInfo objectInfo { get; set; }

    public ILogger Log { get; set; }

    private DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    private bool clearDestTable()
    {
        using (SqlConnection conn = new SqlConnection(this.objectInfo.ConnectionString))
        {
            try
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand($"TRUNCATE TABLE {this.objectInfo.DestinationTableFullName}", conn);
                cmd.ExecuteNonQuery();
                Log.Debug("Очистка таблицы {table} \nSQL:{sql}", this.objectInfo.DestinationTableName, cmd.CommandText);
                Log.Information("Таблица {table} очищена...", this.objectInfo.DestinationTableName);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error("Ошибка при очистке таблицы {table}: \n {ErrMessage}", this.objectInfo.DestinationTableName, ex.Message);
                return false;
            }
        }
    }

    public void BulkImport(bool clearDestTableInAdvance = true)
    {
        if (!clearDestTableInAdvance || (clearDestTableInAdvance && this.clearDestTable()))  
        {
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(this.objectInfo.SourceFilePath, this.dbfOptions))
            {
                Log.Information("Загрузка данных в таблицу {table} ...", this.objectInfo.DestinationTableName);
                using (SqlConnection conn = new SqlConnection(this.objectInfo.ConnectionString))
                {
                    conn.Open();
                    using (var bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = this.objectInfo.DestinationTableName;
                        bulkCopy.BulkCopyTimeout = 60;
                        bulkCopy.BatchSize = 100000;
                        try
                        {
                            bulkCopy.WriteToServer(dbfDataReader);
                            Log.Information("Загрузка данных в таблицу {table} завершена", this.objectInfo.DestinationTableName);
                        }
                        catch (Exception ex)
                        {
                            Log.Error("Ошибка импорта dbf-файла {file} : \n {message}", this.objectInfo.SourceFilePath, ex.Message);
                        }
                    }
                }
            }
        }
    }
}
