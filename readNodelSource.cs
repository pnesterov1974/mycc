using System.Data;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;
using Shared.connections;

namespace Dataread.mssql
{
    public class Source
    {
        public string DbName
        {
            get => this.ParentModel.ReadModel.SourceDbName;
        }
        public string ConnectionString { get; set; } = string.Empty;
        public bool ConnectionOk { get; set; } = false;
        public List<string> KeyFields
        {
            get => this.ParentModel.ReadModel.SourceKeyFilelds;
        }
        public string SelectSql
        {
            get => this.ParentModel.ReadModel.SourceSql;
        }
        // private DbStru? dbs;
        // public DbStru Dbs
        // {
        //     get
        //     {
        //         if (this.dbs != null)
        //         {
        //             return this.dbs;
        //         }
        //         else
        //         {
        //             throw new NullReferenceException("Объект Source.dbs не инициализирован");
        //         }
        //     }
        // }
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
                    throw new NullReferenceException("runModelSource.parentModel: Отсутствует указатель на родительский объект TunModel parentModel");
                }
            }
        }

        // private DataTable? sourceDbStru;
        // public DataTable SourceDbStru
        // {
        //     get
        //     {
        //         if (this.sourceDbStru != null)
        //         {
        //             return this.sourceDbStru;
        //         }
        //         else
        //         {
        //             throw new NullReferenceException("etlModelSource.sourceDbStru: sourceDbStru не инициализирован");
        //         }
        //     }
        // }

        public Source(RunModel parentModel)
        {
            this.parentModel = parentModel;
            this.ConnectionString = ConnectionStrings.GetConnectionStringByName(this.DbName);
            //var msSqlConn = new MsSqlConnection(this.ConnectionString);
        }
    }
}

        // private void readSourceDBStru()
        // {
        //     Log.Information("Загрузка схемы данных источника {source}", this.DbName);
        //     Log.Debug("SQL:\n{sql}", this.SelectSql);

        //     this.sourceDbStru = new DataTable();
        //     using (SqlConnection conn = new SqlConnection(this.ConnectionString))
        //     {
        //         using (SqlCommand cmd = new SqlCommand(this.SelectSql, conn))
        //         {
        //             cmd.CommandTimeout = 100000;
        //             try
        //             {
        //                 conn.Open();
        //                 using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo))
        //                 {
        //                     this.sourceDbStru = rdr.GetSchemaTable();
        //                     Log.Information("Схема данных успешно загружена");
        //                     this.dbs = new DbStru(this.SourceDbStru);
        //                 }
        //             }
        //             catch (Exception ex)
        //             {
        //                 Log.Error("Ошибка загрузки схемы данных.");
        //                 Log.Debug("{errmes}", ex.Message);
        //                 this.sourceDbStru = null;
        //             }
        //         }
        //     }
        // }
