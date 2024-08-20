using Npgsql;
using Shared;

namespace KladrData.DbfToPg;

public class PgImportAltNames : PgImportBase
{
    public PgImportAltNames(ObjectInfo objectInfo) : base(objectInfo)
    {
    }

    public void DoImport(bool clearDestTableInAdvance = true, int BufferRecs = 100000)
    {
        int bufferRecs;
        if (this.objectInfo.BufferRecs > 0)
        {
            bufferRecs = this.objectInfo.BufferRecs;
        }
        else
        {
            bufferRecs = BufferRecs;
        }

        if (!clearDestTableInAdvance || (clearDestTableInAdvance && this.clearDestTable()))
        {
            Log.Information("Начинаю импорт данных в {table}", this.objectInfo.DestinationTableName);
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(this.objectInfo.SourceFilePath, this.dbfOptions))
            {
                int RecordCount = 0;
                int CurBufferRecs = 0;
                using var conn = new NpgsqlConnection(this.objectInfo.ConnectionString);
                conn.Open();
                //var t = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                var batch = new NpgsqlBatch(conn);
                DateTime dtStart = DateTime.Now;

                while (dbfDataReader.Read())
                {   // AltNames
                    var oldcode = dbfDataReader.GetString(0);
                    var newcode = dbfDataReader.GetString(1);
                    var level = dbfDataReader.GetString(2);

                    var batchCommand = new NpgsqlBatchCommand("INSERT INTO public.altnames(oldcode, newcode, level) VALUES (@oldcode, @newcode, @level);");

                    batchCommand.Parameters.AddWithValue("@oldcode", oldcode);
                    batchCommand.Parameters.AddWithValue("@newcode", newcode);
                    batchCommand.Parameters.AddWithValue("@level", level);
                    RecordCount += 1;
                    batch.BatchCommands.Add(batchCommand);
                    CurBufferRecs += 1;
                    if (CurBufferRecs >= bufferRecs)
                    {
                        batch.ExecuteNonQuery();
                        Log.Information("{table} import: imported {recs}", this.objectInfo.DestinationTableName, RecordCount);
                        CurBufferRecs = 0;
                        batch.BatchCommands.Clear();
                    }
                }
                if (CurBufferRecs > 0)
                {
                    batch.ExecuteNonQuery();
                    Log.Information("{table} import: imported {recs}", this.objectInfo.DestinationTableName, RecordCount);
                    batch.BatchCommands.Clear();
                }
                DateTime dtFinish = DateTime.Now;
                TimeSpan duration = dtFinish - dtStart;
                Log.Information("{table} import duration: {duration}, imported {recs}", this.objectInfo.DestinationTableName, duration, RecordCount);
            }
        }
    }
}
