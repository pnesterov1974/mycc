using System.Data;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using static Shared.MyLogger;
using Shared;
using System.Text;

namespace Datamech.mssql
{
    public class RunModel
    {
        public IEtlModel EtlModel;
        private DataTable sourceStru;
        private string sourceConnectionString;
        private bool SourceAndTargetSameConnection = false;
        private bool sourceConnectionOk = false;
        private bool targetConnectionOk = false;
        private bool isTargetObjectExists = false;
        private string targetConnectionString;
        public RunModel(IEtlModel etlModel)
        { // try-finally
            this.EtlModel = etlModel;
            this.prepareModelToRun();
        }

        private void prepareModelToRun()
        {
            Connections conns = new Connections(TargetDb.mssql);
            this.SourceAndTargetSameConnection = this.EtlModel.SourceName.Equals(this.EtlModel.TargetName);
            this.sourceConnectionString = conns.GetConnectionStringByName(this.EtlModel.SourceName);
            this.sourceConnectionOk = this.tryMsSqlDbConnection(this.sourceConnectionString);
            this.targetConnectionString = conns.GetConnectionStringByName(this.EtlModel.TargetName);
            this.targetConnectionOk = this.tryMsSqlDbConnection(this.targetConnectionString);
            this.isTargetObjectExists = this.getIsTargetObjectExists();
        }

        private void prepareTargetObject()
        {
            getSourceDBStru();
            if (this.sourceStru != null)
            {
                Dictionary<string, string> fl = this.getFieldList();
                foreach (var kv in fl)
                {
                    Log.Information("k: {k}\t v: {v}", kv.Key, kv.Value);
                }
                string crt = this.getCreateObjectSql(fl);
                Log.Information("{crt}", crt);
                this.executeCreateObject(crt);
            }
        }

        public void DoRun()
        {
            if (this.sourceConnectionOk && this.targetConnectionOk)
            {
                this.prepareTargetObject();
                this.runModelUsingBulk();
            }
        }

        private void runModelUsingBulk()
        {
            using (SqlConnection sourceConn = new SqlConnection(this.sourceConnectionString))
            {
                sourceConn.Open();
                SqlCommand sourceCmd = new SqlCommand(this.EtlModel.SourceSql, sourceConn);
                SqlDataReader sourceDataReader = sourceCmd.ExecuteReader();
                using (SqlConnection targetConn = new SqlConnection(this.targetConnectionString))
                {
                    targetConn.Open();
                    using (var bulkCopy = new SqlBulkCopy(targetConn))
                    {
                        bulkCopy.DestinationTableName = this.EtlModel.TargetTableFullName;
                        bulkCopy.BulkCopyTimeout = 60;
                        bulkCopy.BatchSize = 100000;
                        try
                        {
                            bulkCopy.WriteToServer(sourceDataReader);
                            Log.Information("Загрузка данных в таблицу {table} завершена", this.EtlModel.TargetTableFullName);
                        }
                        catch (Exception ex)
                        {
                            Log.Error("Ошибка etl для целевого объекта {target}: \n {message}", this.EtlModel.TargetTableFullName, ex.Message);
                        }
                    }
                }
            }
        }

        public int RunModelUsingBatches()
        {
            int flushBuffer = 100000;
            int CurBufferRecs = 0;
            int RecordCount = 0;
            using (SqlConnection sourceConn = new SqlConnection(this.sourceConnectionString))
            {
                sourceConn.Open();
                SqlCommand sourceCmd = new SqlCommand(this.EtlModel.SourceSql, sourceConn);
                SqlDataReader sourceDataReader = sourceCmd.ExecuteReader();

                using (SqlConnection targetConn = new SqlConnection(this.targetConnectionString))
                {
                    var batch = new SqlBatch();

                    while (sourceDataReader.Read())
                    {
                        var name1 = sourceDataReader.GetString(0);
                        var name2 = sourceDataReader.GetString(1);
                        var name3 = sourceDataReader.GetString(2);

                        var bcmd = new SqlBatchCommand(
                            $"""
                            INSERT INTO {this.EtlModel.TargetTableFullName}(name1, name2, name3) VALUES (@name1, @name2, @name3);
                            """
                        );

                        SqlParameter pName1 = new SqlParameter
                        {
                            ParameterName = "@name1",
                            Direction = ParameterDirection.Input,
                            SqlValue = name1,
                            SqlDbType = SqlDbType.Text
                        };

                        bcmd.Parameters.Add(pName1);

                        batch.BatchCommands.Add(bcmd);

                        CurBufferRecs += 1;
                        if (CurBufferRecs >= flushBuffer)
                        {
                            RecordCount += batch.ExecuteNonQuery();
                            Log.Information("{table} загружено {recs} записей", this.EtlModel.TargetTableName, RecordCount);
                            CurBufferRecs = 0;
                            batch.BatchCommands.Clear();
                        }
                    }
                    if (CurBufferRecs > 0)
                    {
                        RecordCount += batch.ExecuteNonQuery();
                        Log.Information("{table} загружено {recs} записей", this.EtlModel.TargetTableName, RecordCount);
                        batch.BatchCommands.Clear();
                    }

                }
            }
            return RecordCount;
        }

        private void getSourceDBStru() // Определение схемы НД источника, по которому можно сделать CREATE целевой объект
        {
            Log.Information("Загрузка схемы данных источника {source}", EtlModel.SourceName);
            Log.Debug("По строке подключения: \n{connstr}", this.sourceConnectionString);
            Log.Debug("SQL : \n{connstr}", this.EtlModel.SourceSql);

            this.sourceStru = new DataTable();
            using (SqlConnection conn = new SqlConnection(this.sourceConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(this.EtlModel.SourceSql, conn))
                {
                    cmd.CommandTimeout = 100000;
                    try
                    {
                        conn.Open();
                        using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                        {
                            this.sourceStru = rdr.GetSchemaTable();
                            Log.Information("Схема данных успешно загружена");
                            string json = JsonConvert.SerializeObject(this.sourceStru, Formatting.Indented);
                            Log.Debug("DBStru in json format:\n{json}", json);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ошибка загрузки схемы данных.");
                        Log.Debug("{errmes}", ex.Message);
                        this.sourceStru = null;
                    }
                }
            }
        }

        public Dictionary<string, string> getFieldList()
        {
            int ColumnNameColIdx = this.sourceStru.Columns["BaseColumnName"].Ordinal;
            int DataTypeNameColIdx = this.sourceStru.Columns["DataTypeName"].Ordinal;
            int ColumnSizeColIdx = this.sourceStru.Columns["ColumnSize"].Ordinal;

            Dictionary<string, string> Fields = new Dictionary<string, string>();

            Log.Information("first: {col} second: {type}", ColumnNameColIdx, DataTypeNameColIdx);
            foreach (DataRow dataRow in this.sourceStru.Rows)
            {
                string columnName = dataRow[ColumnNameColIdx].ToString();
                string columnType = dataRow[DataTypeNameColIdx].ToString();
                string columnSize = dataRow[ColumnSizeColIdx].ToString();

                string columnTypeSized = string.Empty;

                if (columnType.Equals("varchar") || columnType.Equals("nvarchar"))
                {
                    columnTypeSized = $"{columnType}({columnSize})";
                }
                else
                {
                    columnTypeSized = columnType;
                }
                Fields.Add(columnName, columnTypeSized);
                Log.Debug("columnName: {col} columnType: {type}", columnName, columnTypeSized);
            }
            return Fields;
        }
        private bool getIsTargetObjectExists()
        {
            string checkSql = $"""
                IF OBJECT_ID(N'{this.EtlModel.TargetTableFullName}', N'U') IS NOT NULL
                    SELECT 1 AS res
                ELSE SELECT 0 AS res;
            """;
            Log.Information("Проверка, существует ли уже объект {t}", this.EtlModel.TargetTableFullName);
            using (SqlConnection conn = new SqlConnection(this.targetConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(checkSql, conn))
                {
                    try
                    {
                        conn.Open();
                        var result = cmd.ExecuteScalar();
                        int iresult = (int)result;
                        Log.Information("result {r}", iresult);
                        return iresult == 1;
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ошибка загрузки схемы данных.");
                        Log.Debug("{errmes}", ex.Message);
                        return false;
                    }
                }
            }
        }

        private string getInsertRecordSql(Dictionary<string, string> fields)
        {
            return string.Empty;
        }

        private string getCreateObjectSql(Dictionary<string, string> fields)
        {
            StringBuilder sb = new StringBuilder();
            List<string> lfields = new List<string>();

            sb.AppendLine($"CREATE TABLE {this.EtlModel.TargetTableFullName}");
            foreach (var item in fields)
            {
                lfields.Add(
                    string.Concat("[", item.Key, "] ", item.Value.ToUpper(), " NULL")
                );
            }
            string bottomString = string.Concat('(', string.Join(",\n", lfields), ") ON [PRIMARY]");
            sb.Append(bottomString);
            return sb.ToString();
        }
        private bool executeCreateObject(string createSql)
        {
            Log.Information("Создание целевой таблицы ...");
            Log.Debug("SQL: \n{}", createSql);
            using (SqlConnection conn = new SqlConnection(this.targetConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(createSql, conn))
                {
                    try
                    {
                        conn.Open();
                        cmd.ExecuteNonQuery();
                        Log.Error("Целевой объект {target} успешно создан", this.EtlModel.TargetTableFullName);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Ошибка создания целевого объекта {target}", this.EtlModel.TargetTableFullName);
                        Log.Debug("{errmes}", ex.Message);
                        return false;
                    }
                }
            }
        }
        private bool tryMsSqlDbConnection(string connectionString)
        {
            Log.Information("Проверка подключения по строке: {@connString}", connectionString);
            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    string connInfo = this.getMsSqlConnectionInformation(conn);
                    Log.Debug("Информация о соединении:\n {conn}", connInfo);
                    using (SqlCommand cmd = new SqlCommand("SELECT GETDATE();", conn))
                    {
                        cmd.CommandType = CommandType.Text;
                        var dt = cmd.ExecuteScalar();
                        Log.Information("Подключено успешно...{dt}", dt);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("Ошибка проверки подключения с Базе Данных:\n SQL:{message}", ex.Message);
                return false;
            }
        }
        private string getMsSqlConnectionInformation(SqlConnection cnn)
        {
            StringBuilder sb = new StringBuilder(1024);

            sb.AppendLine("Connection String: " + cnn.ConnectionString);
            sb.AppendLine("State: " + cnn.State.ToString());
            sb.AppendLine("Connection Timeout: " + cnn.ConnectionTimeout.ToString());
            sb.AppendLine("Database: " + cnn.Database);
            sb.AppendLine("Data Source: " + cnn.DataSource);
            sb.AppendLine("Server Version: " + cnn.ServerVersion);
            sb.AppendLine("Workstation ID: " + cnn.WorkstationId);

            return sb.ToString();
        }
    }
}
