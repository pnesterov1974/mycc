using Npgsql;
using Shared;

namespace KladrData.DbfToPg;

public class PgImportSocrBase : PgImportBase
{
    public PgImportSocrBase(ObjectInfo objectInfo) : base(objectInfo)
    {
    }

    public void DoImport(bool clearDestTableInAdvance = true)
    {
        if (!clearDestTableInAdvance || (clearDestTableInAdvance && this.clearDestTable()))
        {
            Log.Information("Начинаю импорт данных в {table}", this.objectInfo.DestinationTableName);
            using (var dbfDataReader = new DbfDataReader.DbfDataReader(this.objectInfo.SourceFilePath, this.dbfOptions))
            {
                var dataSourceBuilder = new NpgsqlDataSourceBuilder(this.objectInfo.ConnectionString);
                var dataSource = dataSourceBuilder.Build();

                DateTime dtStart = DateTime.Now;

                using NpgsqlConnection conn = dataSource.OpenConnection();
                //var t = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);

                int RecordCount = 0;
                while (dbfDataReader.Read())
                {   // SocrName
                    var level = dbfDataReader.GetString(0);
                    var scname = dbfDataReader.GetString(1);
                    var socrname = dbfDataReader.GetString(2);
                    var kod_t_st = dbfDataReader.GetString(3);

                    NpgsqlCommand cmd = dataSource.CreateCommand(
                        "INSERT INTO public.socrbase(level, scname, socrname, kod_t_st) VALUES (@level, @scname, @socrname, @kod_t_st);"
                    );

                    cmd.Parameters.AddWithValue("@level", level);
                    cmd.Parameters.AddWithValue("@scname", scname);
                    cmd.Parameters.AddWithValue("@socrname", socrname);
                    cmd.Parameters.AddWithValue("@kod_t_st", kod_t_st);
                    RecordCount += 1;
                    Log.Debug(new string('=', 20));
                    Log.Debug(cmd.ToString());
                    Log.Debug(new string('=', 20));
                    cmd.ExecuteNonQuery();
                }
                //t.Commit();
                //conn.Close();
                DateTime dtFinish = DateTime.Now;
                TimeSpan duration = dtFinish - dtStart;
                Log.Information("SOCRBASE import duration: {duration}, imported {recs}", duration, RecordCount);
            }
        }
    }
}
