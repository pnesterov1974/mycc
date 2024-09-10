using Microsoft.Data.SqlClient;
using DbfDataReader;
using Shared;
using static Shared.MyLogger;
using Data;

namespace Import.kladr.mssql;

public class MsSqlImport : BaseMsSql
{
    public ImportObjectInfo ObjectInfo { get; set; }

    public MsSqlImport(ImportObjectInfo objectInfo) => this.ObjectInfo = objectInfo;

    public void BulkImport(bool clearDestTableInAdvance = true)
    {
        if (!clearDestTableInAdvance ||
                (clearDestTableInAdvance &&
                    this.ClearDestTable(
                        this.ObjectInfo.ConnectionString,
                        this.ObjectInfo.TargetTableFullName
                )
            )
        )
        {
            DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(this.ObjectInfo.SourceFilePath, dbfOptions))
            {
                Log.Information("Загрузка данных в таблицу {table} ...", this.ObjectInfo.TargetTableFullName);
                using (SqlConnection conn = new SqlConnection(this.ObjectInfo.ConnectionString))
                {
                    conn.Open();
                    using (var bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = this.ObjectInfo.TargetTableFullName;
                        bulkCopy.BulkCopyTimeout = 60;
                        bulkCopy.BatchSize = 100000;
                        try
                        {
                            bulkCopy.WriteToServer(dbfDataReader);
                            Log.Information("Загрузка данных в таблицу {table} завершена", this.ObjectInfo.TargetTableFullName);
                        }
                        catch (Exception ex)
                        {
                            Log.Error("Ошибка импорта dbf-файла {file} : \n {message}", this.ObjectInfo.SourceFilePath, ex.Message);
                        }
                    }
                }
            }
        }
    }
}
