using static Shared.MyLogger;
using Shared.connections;
using Shared;

namespace Datamech
{
    public class ValidateModel
    {
        private TargetDb targetDb;
        private IEtlModel etlModel;
        public IEtlModel EtlModel
        {
            get
            {
                if (this.etlModel != null)
                {
                    return this.etlModel;
                }
                else
                {
                    throw new NullReferenceException("ValidatreModel: etlModel не инициализирован");
                }
            }
        }

        public ValidateModel(TargetDb tdb, IEtlModel etlModel)
        {
            this.targetDb = tdb;
            if (etlModel != null)
            {
                this.etlModel = etlModel;
            }
            else
            {
                throw new ArgumentNullException("ValidateEtlModel: в конструктор передана EtlModel: null");
            }
        }

        public bool Validate(bool useKeyFieldsWhenValidating = false)
        {
            Log.Information("Начало валидации модели");

            bool modelHasName = !string.IsNullOrEmpty(this.EtlModel.ModelName);
            if (modelHasName)
            {
                Log.Information("Модель имеет корректное имя {modelName}", this.EtlModel.ModelName);
            }
            else
            {
                Log.Error("Модель НЕ имеет корректное имя {modelName}", this.EtlModel.ModelName);
            }

            bool modelHasSourceDbName = !string.IsNullOrEmpty(this.EtlModel.SourceDbName);
            bool modelHasSourceDbConnectionString = false;
            bool modelSourceConnectionIsValid = false;
            bool modelSourceSelectSqlIsValid = false;
            if (modelHasSourceDbName)
            {
                Log.Information("Модель имеет название БД-источника {sourceDbName}", this.EtlModel.SourceDbName);
                try
                {
                    string connStr = ConnectionStrings.GetConnectionStringByName(this.EtlModel.SourceDbName);
                    modelHasSourceDbConnectionString = (connStr.Length > 0);
                    Log.Information("Название БД-источника {sourceDbName} корректно", this.EtlModel.SourceDbName);

                    switch (this.targetDb)
                    {
                        case TargetDb.mssql:
                            var msSqlConn = new MsSqlConnection(connStr);
                            modelSourceConnectionIsValid = msSqlConn.TryMsSqlDbConnection();
                            if (modelSourceConnectionIsValid)
                            {
                                modelSourceSelectSqlIsValid =  msSqlConn.CheckMsSqlSelectSql(this.EtlModel.SourceSql);
                            }
                            break;
                        case TargetDb.pgsql:
                            var pgConn = new PgConnection(connStr);
                            modelSourceConnectionIsValid = pgConn.TryPgSqlDbConnection();
                            if (modelSourceConnectionIsValid)
                            {
                                modelSourceSelectSqlIsValid =  pgConn.CheckPgSelectSql(this.EtlModel.SourceSql);
                            }
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Log.Information("Название БД-источника {sourceDbName} НЕ корректно. Отутствует в справочнике подключений", this.EtlModel.SourceDbName);
                    modelHasSourceDbConnectionString = false;
                }
            }
            else
            {
                Log.Error("Модель НЕ имеет корректного названия БД-источника {sourceDbName}", this.EtlModel.SourceDbName);
            }

            bool modelHasSelectSql = !string.IsNullOrEmpty(this.EtlModel.SourceSql);
            if (modelHasSelectSql)
            {
                Log.Information("Модель имеет SelectSql");
                Log.Debug("\n {selectDql}", this.EtlModel.SourceSql);
                // Проверка валидность sql (переместить вверх)  <- нужен проброс mssql / pgsql
            }
            else
            {
                Log.Error("Модель НЕ имеет SelectSql");
            }


            bool modelHasTargetDbName = !string.IsNullOrEmpty(this.EtlModel.TargetDbName);
            bool modelHasTargetDbConnectionString = false;
            bool modelTargetConnectionIsValid = false;
            if (modelHasTargetDbName)
            {
                Log.Information("Модель имеет корректное название БД-приемника {targetDbName}", this.EtlModel.TargetDbName);
                if (modelHasTargetDbName)
                {
                    Log.Information("Модель имеет название БД-приемника {targetDbName}", this.EtlModel.TargetDbName);
                    try
                    {
                        string connStr = ConnectionStrings.GetConnectionStringByName(this.EtlModel.TargetDbName);
                        modelHasTargetDbConnectionString = (connStr.Length > 0);
                        Log.Information("Название БД-приемника {targetDbName} корректно", this.EtlModel.TargetDbName);
                        switch (this.targetDb)
                        {
                            case TargetDb.mssql:
                                var msSqlConn = new MsSqlConnection(connStr);
                                modelTargetConnectionIsValid = msSqlConn.TryMsSqlDbConnection();
                                break;
                            case TargetDb.pgsql:
                                var pgConn = new PgConnection(connStr);
                                modelTargetConnectionIsValid = pgConn.TryPgSqlDbConnection();
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Information("Название БД-приемника {targetDbName} НЕ корректно. Отутствует в справочнике подключений", this.EtlModel.SourceDbName);
                        modelHasTargetDbConnectionString = false;
                    }
                }
                else
                {
                    Log.Error("Модель НЕ имеет корректного названия БД-приемника {targetDbName}", this.EtlModel.TargetDbName);
                }
            }
            else
            {
                Log.Error("Модель НЕ имеет корректного названия БД-приемника {targetDbName}", this.EtlModel.TargetDbName);
            }

            bool modelHasTargetTableName = !string.IsNullOrEmpty(this.EtlModel.TargetTableName);
            if (modelHasTargetTableName)
            {
                Log.Information("Модель имеет корректное название целевой таблицы {targetTableName}", this.EtlModel.TargetTableName);
            }
            else
            {
                Log.Error("Модель НЕ имеет корректного названия целевой тпблицы {targetTableName}", this.EtlModel.TargetTableName);
            }

            bool modelHasTargetSchemaName = !string.IsNullOrEmpty(this.EtlModel.TargetSchemaName);
            if (modelHasTargetSchemaName)
            {
                Log.Information("Модель имеет корректное название целевой схемы {targetSchemaName}", this.EtlModel.TargetSchemaName);
            }
            else
            {
                Log.Error("Модель НЕ имеет корректного названия целевой схемы {targetSchemaName}", this.EtlModel.TargetSchemaName);
            }

            bool modelHasKeyList = (this.EtlModel.SourceKeyFilelds != null) && (this.EtlModel.SourceKeyFilelds.Count > 0);
            if (modelHasKeyList)
            {

                string keyFields = string.Join(", ", this.EtlModel.SourceKeyFilelds);
                Log.Information("Модель имеет список ключевых полей:");
                Log.Information(keyFields);

            }
            else
            {
                Log.Error("Модель НЕ имеет списка ключевых полей");
            }


            bool result = modelHasName && modelHasSourceDbName && modelHasTargetDbName &&
                          modelHasTargetSchemaName && modelHasTargetTableName && modelHasSelectSql &&
                          modelHasSourceDbConnectionString && modelSourceConnectionIsValid && modelSourceSelectSqlIsValid &&
                          modelHasTargetDbConnectionString && modelTargetConnectionIsValid;

            if (useKeyFieldsWhenValidating)
            {
                result = result && modelHasKeyList;
            }

            Log.Information("Валидация модели закончена, {result}", result ? "Ok" : "Модкль не валидна !");
            return result;
        }
    }
}
