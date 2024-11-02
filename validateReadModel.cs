using static Shared.MyLogger;
using Shared.connections;
using Shared;

namespace Dataread
{
    public class ValidateModel
    {
        private TargetDb targetDb;
        private IReadModel readModel;
        public IReadModel ReadModel
        {
            get
            {
                if (this.readModel != null)
                {
                    return this.readModel;
                }
                else
                {
                    throw new NullReferenceException("ValidatreModel: etlModel не инициализирован");
                }
            }
        }

        public ValidateModel(TargetDb tdb,  IReadModel etlModel)
        {
            this.targetDb = tdb;
            if (etlModel != null)
            {
                this.readModel = etlModel;
            }
            else
            {
                throw new ArgumentNullException("ValidateEtlModel: в конструктор передана EtlModel: null");
            }
        }

        public bool Validate()
        {
            Log.Information("Начало валидации модели");

            bool modelHasName = !string.IsNullOrEmpty(this.ReadModel.ModelName);
            if (modelHasName)
            {
                Log.Information("Модель имеет корректное имя {modelName}", this.ReadModel.ModelName);
            }
            else
            {
                Log.Error("Модель НЕ имеет корректное имя {modelName}", this.ReadModel.ModelName);
            }

             bool modelHasSelectSql = !string.IsNullOrEmpty(this.ReadModel.SourceSql);
            if (modelHasSelectSql)
            {
                Log.Information("Модель имеет SelectSql");
                Log.Debug("\n {selectDql}", this.ReadModel.SourceSql);
            }
            else
            {
                Log.Error("Модель НЕ имеет SelectSql");
            }

            bool modelHasSourceDbName = !string.IsNullOrEmpty(this.ReadModel.SourceDbName);
            bool modelHasSourceDbConnectionString = false;
            bool modelSourceConnectionIsValid = false;
            bool modelSourceSelectSqlIsValid = false;
            if (modelHasSourceDbName)
            {
                Log.Information("Модель имеет название БД-источника {sourceDbName}", this.ReadModel.SourceDbName);
                try
                {
                    string conn = ConnectionStrings.GetConnectionStringByName(this.ReadModel.SourceDbName);
                    modelHasSourceDbConnectionString = (conn.Length > 0);
                    Log.Information("Название БД-источника {sourceDbName} корректно", this.ReadModel.SourceDbName);
                   
                    switch (this.targetDb)
                    {
                        case TargetDb.mssql:
                            var msSqlConn = new MsSqlConnection(conn);
                            modelSourceConnectionIsValid = msSqlConn.TryMsSqlDbConnection();
                            if (modelSourceConnectionIsValid)
                            {
                                modelSourceSelectSqlIsValid =  msSqlConn.CheckMsSqlSelectSql(this.ReadModel.SourceSql);
                            }
                            break;
                        case TargetDb.pgsql:
                            var pgConn = new PgConnection(conn);
                            modelSourceConnectionIsValid = pgConn.TryPgSqlDbConnection();
                            if (modelSourceConnectionIsValid)
                            {
                                modelSourceSelectSqlIsValid =  pgConn.CheckPgSelectSql(this.ReadModel.SourceSql);
                            }
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Log.Information("Название БД-источника {sourceDbName} НЕ корректно. Отутствует в справочнике подключений", this.ReadModel.SourceDbName);
                    modelHasSourceDbConnectionString = false;
                }
            }
            else
            {
                Log.Error("Модель НЕ имеет корректного названия БД-источника {sourceDbName}", this.ReadModel.SourceDbName);
            }

            bool result = modelHasName && modelHasSourceDbName && modelHasSelectSql && modelSourceSelectSqlIsValid &&
                          modelHasSourceDbConnectionString && modelSourceConnectionIsValid;

            Log.Information("Валидация модели закончена, {result}", result ? "Ok" : "Модкль не валидна !");
            return result;
        }
    }
}
