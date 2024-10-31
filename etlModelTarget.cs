using System.Data;
using Microsoft.Data.SqlClient;
using Shared.connections;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class Target//: SourceTargetBase
    {
        public string DbName
        {
            get => this.parentModel.EtlModel.TargetDbName;
        }
        public string ConnectionString { get; set; } = string.Empty;
        public bool ConnectionOk { get; set; } = false;
        public List<string> KeyFields
        {
            get => this.parentModel.EtlModel.SourceKeyFilelds;
        }
        public string SchemaName
        {
            get => this.parentModel.EtlModel.TargetSchemaName;
        }
        public string TableName
        {
            get => this.parentModel.EtlModel.TargetTableName;
        }
        public string FullName
        {
            get => string.Join('.', this.SchemaName, this.TableName);
        }
        private bool isObjectExists;
        private RunModel parentModel;

        public Target(RunModel parentModel)
        {
            this.parentModel = parentModel;
            this.ConnectionString = ConnectionStrings.GetConnectionStringByName(this.DbName);
            var msSqlConn = new MsSqlConnection(this.ConnectionString);
            this.ConnectionOk = msSqlConn.TryMsSqlDbConnection();
        }

        public void ReadIfObjectExists()
        {
            string checkSql = this.parentModel.Sqls.GetIsObjectExistsSql();
            Log.Information("Проверка, существует ли уже объект {t}", this.FullName);
            Log.Debug("checkSql:\n{checkSql}", checkSql);
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(checkSql, conn))
                {
                    try
                    {
                        conn.Open();
                        var result = cmd.ExecuteScalar();
                        int iresult = (int)result;
                        Log.Debug("Результат проверки: result {r}", iresult);
                        if (iresult == 1)
                        {
                            Log.Information("Объект {t} существует", this.FullName);
                            this.isObjectExists = true;
                        }
                        else
                        {
                            Log.Information("Объект {t} не существует", this.FullName);
                            this.isObjectExists = false;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ощибка проверки существования объекта {t}\n", this.FullName);
                        Log.Debug("{errmes}", ex.Message);
                        this.isObjectExists = false;
                    }
                }
            }
        }

        public void TouchObject(bool deleteIfExists)
        {
            if (this.isObjectExists)
            {
                if (deleteIfExists)
                {
                    this.dropTable();
                    this.createTable();
                }
            }
            else
            {
                this.createTable();
            }
        }

        private void dropTable()
        {
            string dropSql = this.parentModel.Sqls.GetDropTableSql();
            Log.Information("Удаление таблицы {t}", this.FullName);
            Log.Debug("checkSql:\n{checkSql}", dropSql);
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(dropSql, conn))
                {
                    try
                    {
                        conn.Open();
                        int result = cmd.ExecuteNonQuery();
                        Log.Information("Объект {t} успешно удален", this.FullName);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ощибка удаления объекта {t}\n", this.FullName);
                        Log.Debug("{errmes}", ex.Message);
                    }
                }
            }
        }

        private void createTable()
        {
            Log.Information("Создание таблицы {t}", this.FullName);
            string createSql = this.parentModel.Sqls.GetCreateTableSql();
            Log.Debug("createSql:\n{createSql}", createSql);
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(createSql, conn))
                {
                    try
                    {
                        conn.Open();
                        int result = cmd.ExecuteNonQuery();
                        Log.Information("Объект {t} успешно создан", this.FullName);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ощибка создания объекта {t}\n", this.FullName);
                        Log.Debug("{errmes}", ex.Message);
                    }
                }
            }
        }

        public int SaveBatch(DataTable batchData)
        {
            using SqlConnection conn = new SqlConnection(this.ConnectionString);
            conn.Open();

            using SqlBatch batch = new SqlBatch(conn);

            foreach (DataRow r in batchData.Rows)
            {
                SqlBatchCommand bcmd = new SqlBatchCommand(this.parentModel.Sqls.GetInsertIntoValuesSql());

                foreach (DataColumn c in batchData.Columns)
                {
                    bcmd.Parameters.AddWithValue(string.Concat('@', c.ColumnName), r[c.ColumnName]);
                }
                batch.BatchCommands.Add(bcmd);
            }
            try
            {
                int recs = batch.ExecuteNonQuery();
                return recs;
            }
            catch (Exception ex)
            {
                Log.Information("Ощибка загрузки пачки в {target}", this.FullName);
                Log.Error("\n {errorMsq}", ex.Message);
                return 0;
            }
        }

        public int SaveBatchUsingBulkInsert(DataTable batchData)
        {
            int currentBatchSize = batchData.Rows.Count;
            int batchSize = currentBatchSize + 10;
            using SqlConnection conn = new SqlConnection(this.ConnectionString);
            conn.Open();
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = this.FullName;
                bulkCopy.BulkCopyTimeout = 60;
                bulkCopy.BatchSize = batchSize;
                DataTableReader batchReader = batchData.CreateDataReader();
                try
                {
                    bulkCopy.WriteToServer(batchReader);
                    Log.Information("Загрузка пачки данных размером {recs} в таблицу {table} завершена", currentBatchSize, this.FullName);
                    return currentBatchSize;
                }
                catch (Exception ex)
                {
                    Log.Information("Ошибка Загрузки пачки данных в таблицу {table}", this.FullName);
                    Log.Error("\n", ex.Message);
                    return 0;
                }
            }
        }
    }
}
