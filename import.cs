using Microsoft.Data.SqlClient;
using DbfDataReader;
using static Shared.MyLogger;
using Utils;
using Shared.KladrImportModels;

namespace ImportGeo.kladr.mssql;

public class MsSqlImport : BaseCommonImport
{
    public MsSqlImport()
    {
        this.TargetDb = Shared.TargetDb.mssql;
    }

    public void BulkImport(bool clearDestTableInAdvance = true)
    {
       if (!clearDestTableInAdvance ||
                (clearDestTableInAdvance && 
                    this.DBUtils.ClearDestTable(this.TargetConnectionString, this.TargetTableFullName)
                )
            )
        {
            var utc = new UtilsCommon();
            utc.ReadDbfInfo(this.ImportModel.SourceFullPathName);
            DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(this.ImportModel.SourceFullPathName, dbfOptions))
            {
                Log.Information("Загрузка данных в таблицу {table} ...", this.TargetTableFullName);
                using (SqlConnection conn = new SqlConnection(this.TargetConnectionString))
                {
                    conn.Open();
                    using (var bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = this.TargetTableFullName;
                        bulkCopy.BulkCopyTimeout = 60;
                        bulkCopy.BatchSize = 100000;
                        try
                        {
                            bulkCopy.WriteToServer(dbfDataReader);
                            Log.Information("Загрузка данных в таблицу {table} завершена", this.TargetTableFullName);
                        }
                        catch (Exception ex)
                        {
                            Log.Error("Ошибка импорта dbf-файла {file} : \n {message}", this.ImportModel.SourceFullPathName, ex.Message);
                        }
                    }
                }
            }
        }
    }
}
