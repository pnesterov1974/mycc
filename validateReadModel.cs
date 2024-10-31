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

        public bool Validate(bool useKeyFieldsWhenValidating = false)
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

            bool modelHasSourceDbName = !string.IsNullOrEmpty(this.ReadModel.SourceDbName);
            bool modelHasSourceDbConnectionString = false;
            bool modelSourceConnectionIsValid = false;
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
                            break;
                        case TargetDb.pgsql:
                            var pgConn = new MsSqlConnection(conn);
                            modelSourceConnectionIsValid = pgConn.TryMsSqlDbConnection();
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

            bool modelHasSelectSql = !string.IsNullOrEmpty(this.ReadModel.SourceSql);
            if (modelHasSelectSql)
            {
                Log.Information("Модель имеет SelectSql");
                Log.Debug("\n {selectDql}", this.ReadModel.SourceSql);
                // Проверка валидность sql (переместить вверх)  <- нужен проброс mssql / pgsql
            }
            else
            {
                Log.Error("Модель НЕ имеет SelectSql");
            }

            bool modelHasKeyList = (this.ReadModel.SourceKeyFilelds != null) && (this.ReadModel.SourceKeyFilelds.Count > 0);
            if (modelHasKeyList)
            {

                string keyFields = string.Join(", ", this.ReadModel.SourceKeyFilelds);
                Log.Information("Модель имеет список ключевых полей:");
                Log.Information(keyFields);

            }
            else
            {
                Log.Error("Модель НЕ имеет списка ключевых полей");
            }


            bool result = modelHasName && modelHasSourceDbName && modelHasSelectSql &&
                          modelHasSourceDbConnectionString && modelSourceConnectionIsValid;

            if (useKeyFieldsWhenValidating)
            {
                result = result && modelHasKeyList;
            }

            Log.Information("Валидация модели закончена, {result}", result ? "Ok" : "Модкль не валидна !");
            return result;
        }
    }
}
