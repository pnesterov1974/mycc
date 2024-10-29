using System.Data;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class Source : SourceTargetBase
    {
        public string DbName
        {
            get => this.ParentModel.EtlModel.SourceDbName;
        }
        public List<string> KeyFields
        { 
            get => this.ParentModel.EtlModel.SourceKeyFilelds;
        }
        public string SelectSql 
        { 
            get => this.ParentModel.EtlModel.SourceSql;
        }
        private DbStru? dbs;
        public DbStru Dbs 
        { 
            get
            {
                if (this.dbs != null)
                {
                    return this.dbs;
                }
                else
                {
                    throw new NullReferenceException("Объект Source.dbs не инициализирован");
                }
            } 
        }
        private RunModel parentModel;
        private RunModel ParentModel
        {
            get
            {
                if (this.parentModel != null)
                {
                    return this.parentModel;
                }
                else
                {
                    throw new NullReferenceException("etlModelSource.parentModel: Отсутствует указатель на родительский объект TunModel parentModel");
                }
            }
        }

        private DataTable? sourceDbStru;
        public DataTable SourceDbStru
        { 
            get
            {
                if (this.sourceDbStru != null)
                {
                    return this.sourceDbStru;
                }
                else
                {
                    throw new NullReferenceException("etlModelSource.sourceDbStru: sourceDbStru не инициализирован");
                }
            } 
        }

        public Source(RunModel parentModel)
        {
            if (parentModel == null)
            {
                throw new ArgumentNullException("etlModelSource.sourceDbStru: в конструктор Source передан parentModel: null");
            }
            else
            {
                this.parentModel = parentModel;
                this.ConnectionString = Connections.GetConnectionStringByName(this.DbName);
                this.ConnectionOk = this.tryMsSqlDbConnection();
                if (this.ConnectionOk)
                {
                    this.readSourceDBStru();
                }
            }
        }

        private void readSourceDBStru()
        {
            Log.Information("Загрузка схемы данных источника {source}", this.DbName);
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
                            this.dbs = new DbStru(this.SourceDbStru);
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
