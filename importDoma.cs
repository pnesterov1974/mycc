using Npgsql;
using Shared;

namespace KladrData.DbfToPg;

public class PgImportDoma : PgImportBase
{
    public PgImportDoma(ObjectInfo objectInfo) : base(objectInfo)
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

                var batch = new NpgsqlBatch(conn);
                DateTime dtStart = DateTime.Now;

                while (dbfDataReader.Read())
                {   // Doma
                    var name = dbfDataReader.GetString(0);
                    var korp = dbfDataReader.GetString(1);
                    var socr = dbfDataReader.GetString(2);
                    var code = dbfDataReader.GetString(3);
                    var index = dbfDataReader.GetString(4);
                    var gninmb = dbfDataReader.GetString(5);
                    var uno = dbfDataReader.GetString(6);
                    var ocatd = dbfDataReader.GetString(7);

                    var bcmd = new NpgsqlBatchCommand("INSERT INTO public.doma(name, korp, socr, code, index, gninmb, uno, ocatd) VALUES (@name, @korp, @socr, @code, @index, @gninmb, @uno, @ocatd);");

                    bcmd.Parameters.AddWithValue("@name", name);
                    bcmd.Parameters.AddWithValue("@korp", korp);
                    bcmd.Parameters.AddWithValue("@socr", socr);
                    bcmd.Parameters.AddWithValue("@code", code);
                    bcmd.Parameters.AddWithValue("@index", index);
                    bcmd.Parameters.AddWithValue("@gninmb", gninmb);
                    bcmd.Parameters.AddWithValue("@uno", uno);
                    bcmd.Parameters.AddWithValue("@ocatd", ocatd);

                    RecordCount += 1;
                    batch.BatchCommands.Add(bcmd);
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
