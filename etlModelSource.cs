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
        private DataTable sourceDbStru;
        private RunModel parentModel;
        public DataTable SourceDbStru
        {
            get
            {
                if (this.sourceDbStru == null)
                {
                    this.readSourceDBStru();
                }
                return sourceDbStru;
            }
        }

        public Source(RunModel parentModel)
        {
            this.parentModel = parentModel;
            Connections conns = new Connections(TargetDb.mssql);
            this.ConnectionString = conns.GetConnectionStringByName(this.Name);
            this.ConnectionOk = this.tryMsSqlDbConnection();
        }

        private void readSourceDBStru()
        {
            Log.Information("Загрузка схемы данных источника {source}", this.Name);
            Log.Debug("SQL:\n{sql}", this.SelectSql);

            this.sourceDbStru = new DataTable();
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
                            this.sourceDbStru = rdr.GetSchemaTable();
                            Log.Information("Схема данных успешно загружена");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ошибка загрузки схемы данных.");
                        Log.Debug("{errmes}", ex.Message);
                        this.sourceDbStru = null;
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
    }
}
