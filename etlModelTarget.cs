using System.Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class Target: SourceTargetBase
    {
        public string? Name
        {
            get => this.parentModel?.EtlModel?.TargetName;
        }
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
            Connections conns = new Connections(TargetDb.mssql);
            this.ConnectionString = conns.GetConnectionStringByName(this.Name);
            this.ConnectionOk = this.tryMsSqlDbConnection();
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
            string createSql = this.parentModel.Sqls.GetCreateTableSql();
            Log.Information("Создание таблицы {t}", this.FullName);
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

        public void RunInsertInto()
        {
            string insertIntoSql = this.parentModel.Sqls.GetInsertIntoSql();
            Log.Information("Заполнение таблицы {t}", this.FullName);
            Log.Debug("insertIntoSql:\n{insertIntoSql}", insertIntoSql );
            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(insertIntoSql , conn))
                {
                    try
                    {
                        conn.Open();
                        int result = cmd.ExecuteNonQuery();
                        Log.Information("Объект {t} успешно заполнен, вставлено {r} записей", this.FullName, result);
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ощибка заполнения объекта {t}\n", this.FullName);
                        Log.Debug("{errmes}", ex.Message);
                    }
                }
            }
        }

        public void SaveBatch(DataTable batchData)
        {
            //
        }
    }
}
