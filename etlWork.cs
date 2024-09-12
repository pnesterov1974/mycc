using System.Data;
using System.Text;
using Newtonsoft.Json;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;

namespace Datamech.mssql;

public class EtlWork
{
    public ETLObjectInfo Etlo { get; set; }

    private DataTable sourceStru;

    public void Work()
    {
        bool sourceConnOk = this.tryMsSqlDbConnection(this.Etlo.SourceConnectionString);
        bool targetConnOk = this.tryMsSqlDbConnection(this.Etlo.TargetConnectionString);

        if (targetConnOk)
        {
            bool ex = this.isTargetObjectExists();
        }

        if (sourceConnOk)
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

    private void getSourceDBStru()
    {
        Log.Information("Загрузка схемы данных источника ...");
        Log.Debug("Загрузка схемы из : \n{connstr}", this.Etlo.SourceConnectionString);
        Log.Debug("SQL : \n{connstr}", this.Etlo.SourceSelectSql);
        this.sourceStru = new DataTable();

        using (SqlConnection conn = new SqlConnection(this.Etlo.SourceConnectionString))
        {
            using (SqlCommand cmd = new SqlCommand(this.Etlo.SourceSelectSql, conn))
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

    private bool isTargetObjectExists()
    {
        string checkSql = $"""
            IF OBJECT_ID(N'{this.Etlo.TargetTableFullName}', N'U') IS NOT NULL
                SELECT 1 AS res
            ELSE SELECT 0 AS res;
        """;
        Log.Information("Проверка, существует ли уже объект {t}", this.Etlo.TargetTableFullName);
        using (SqlConnection conn = new SqlConnection(this.Etlo.TargetConnectionString))
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

    private string getCreateObjectSql(Dictionary<string, string> fields)
    {
        StringBuilder sb = new StringBuilder();
        List<string> lfields = new List<string>();

        sb.AppendLine($"CREATE TABLE {this.Etlo.TargetTableFullName}");
        foreach(var item in fields)
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
        using (SqlConnection conn = new SqlConnection(this.Etlo.TargetConnectionString))
        {
            using (SqlCommand cmd = new SqlCommand(createSql, conn))
            {
                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                    Log.Error("Целевой объект {target} успешно создан", Etlo.TargetTableFullName);
                    return true;
                }
                catch (Exception ex)
                {
                    Log.Error("Ошибка создания целевого объекта {target}", Etlo.TargetTableFullName);
                    Log.Debug("{errmes}", ex.Message);
                    return false;
                }
            }
        }
    }
}
