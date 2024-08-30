using Microsoft.Data.SqlClient;
using DbfDataReader;
using Shared;
using static Shared.MyLogger;
using CommonImport;

namespace KladrImport;

public class MsSqlImport: ImportBaseMsSql
{
    public MsSqlImport(ObjectInfo oi): base(oi) {;}
    
    public void BulkImport(bool clearDestTableInAdvance = true)
    {
        if (!clearDestTableInAdvance || (clearDestTableInAdvance && this.clearDestTable()))  
        {
            DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(this.objectInfo.SourceFilePath, dbfOptions))
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
