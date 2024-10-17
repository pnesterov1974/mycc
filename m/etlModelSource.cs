using System.Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class Source : SourceTargetBase
    {
        public string? Name
        {
            get => this.parentModel?.EtlModel?.SourceName;
        }
        public List<string> KeyFields
        { 
            get => this.parentModel.EtlModel.SourceKeyFilelds;
        }
        public string SelectSql 
        { 
            get => this.parentModel.EtlModel.SourceSql;
        }
        public DbStru Dbs { get; set; }
        private RunModel parentModel;
        public DataTable SourceDbStru { get; set; }

        public Source(RunModel parentModel)
        {
            this.parentModel = parentModel;
            Connections conns = new Connections(TargetDb.mssql);
            this.ConnectionString = conns.GetConnectionStringByName(this.Name);
            this.ConnectionOk = this.tryMsSqlDbConnection();
            if (this.ConnectionOk)
            {
                this.readSourceDBStru();
            }
        }

        private void readSourceDBStru()
        {
            Log.Information("Загрузка схемы данных источника {source}", this.Name);
            Log.Debug("SQL:\n{sql}", this.SelectSql);

            this.SourceDbStru = new DataTable();
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(this.SelectSql, conn))
                {
                    cmd.CommandTimeout = 100000;
                    try
                    {
                        conn.Open();
                        using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                        {
                            this.SourceDbStru = rdr.GetSchemaTable();
                            Log.Information("Схема данных успешно загружена");
                            this.Dbs = new DbStru(this.SourceDbStru);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ошибка загрузки схемы данных.");
                        Log.Debug("{errmes}", ex.Message);
                        this.SourceDbStru = null;
                    }
                }
            }
        }

        public DataTable ReadBatch(int rnnFrom, int rnnTo, out int RecordsQueried)
        {
            RecordsQueried = 0;
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                conn.Open();
                string selectSql = this.parentModel.Sqls.GetSelectBatchSqlBouded(rnnFrom, rnnTo);
                Log.Debug("selectSql\n {s}", selectSql);
                SqlDataAdapter adapter = new SqlDataAdapter(selectSql, conn);
                DataTable dt = new DataTable();
                RecordsQueried = adapter.Fill(dt);
                return dt;
            }
        }

        public DataTable ReadPage(int pageNum, int pageRecordSize, out int RecordsQueried)
        {
            RecordsQueried = 0;
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                conn.Open();
                string selectSql = this.parentModel.Sqls.GetSelectPageSql(pageNum, pageRecordSize);
                Log.Debug("selectSql\n {s}", selectSql);
                SqlDataAdapter adapter = new SqlDataAdapter(selectSql, conn);
                DataTable dt = new DataTable();
                RecordsQueried = adapter.Fill(dt);
                return dt;
            }
        }

        public IEnumerable<object[]> ReadYield()
        {
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                conn.Open();
                string selectSql = this.parentModel.Source.SelectSql;
                Log.Debug("selectSql\n {s}", selectSql);
                using (SqlCommand cmd = new SqlCommand(selectSql, conn))
                {
                    SqlDataReader dr = cmd.ExecuteReader();
                    if (dr.HasRows)
                    {
                        while(dr.Read())
                        {
                            //var v1 = dr.GetValue(0);
                            //var v2 = dr.GetValue(1);
                            //var v3 = dr.GetValue(2);
                            //string ss = $"{v1} {v2} {v3}";
                            //Log.Information("{v1} {v2} {v3}", v1, v2, v3);
                            //Log.Information(ss);

                            int rowCount = this.Dbs.Rows.Count;
                            object[] sar = new object[rowCount];
                            dr.GetValues(sar);

                            // foreach(var kv in this.Dbs.Rows)
                            // {
                            //     string fieldName = kv.Key;
                            //     DbStruRow rowInfo = kv.Value;
                            //     int ColIdx = rowInfo.ColumnOrdinal;

                            //     var Value = dr.GetValue(ColIdx);
                            //     //dr.GetValues(object[] values)
                            // }

                            yield return sar;
                        }
                    }
                }
            }
        }
    }
}
