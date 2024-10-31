using System.Data;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using static Shared.MyLogger;

namespace Data.mesh.mssql;

public class MsSqlSchema
{
     public DataTable? Data { get; set; } = null;

    protected string connectionString { get; set; } = string.Empty;
    protected string selectSql { get; set; } = string.Empty;

    public string AsJson 
    { 
        get
        {
            if (this.Data == null)
            {
                this.Read(); // reread data
            }
            return JsonConvert.SerializeObject(this.Data, Formatting.Indented);
        }
    }

    public MsSqlSchema(string connectionString, string selectSql)
    {
        this.connectionString = connectionString;
        this.selectSql = selectSql;
    }

    public void Read()
    {
        Log.Information("Загрузка схемы...");
        Log.Debug("Загрузка схемы из : \n{connstr}", this.connectionString);
        Log.Debug("SQL : \n{connstr}", this.selectSql);
        this.Data = new DataTable();

        using (SqlConnection conn = new SqlConnection(this.connectionString))
        {
            using (SqlCommand cmd = new SqlCommand(this.selectSql, conn))
            {
                try
                {
                    conn.Open();
                    using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        this.Data = rdr.GetSchemaTable();
                        Log.Information("Схема загружена");
                    }
                }
                catch (Exception ex)
                {
                    Log.Debug("Ошибка чтения даннтых \n {errmes}", ex.Message);
                }
            }
        }
    }

    public Dictionary<string, string> GetFieldList()
    {
        int ColumnNameColIdx = this.Data.Columns["BaseColumnName"].Ordinal;
        int DataTypeNameColIdx = this.Data.Columns["DataTypeName"].Ordinal;
        int ColumnSizeColIdx = this.Data.Columns["ColumnSize"].Ordinal;

        Dictionary<string, string> Fields = new Dictionary<string, string>();

        Log.Information("first: {col} second: {type}", ColumnNameColIdx, DataTypeNameColIdx);
        foreach (DataRow dataRow in this.Data.Rows)
        {
            string columnName = dataRow[ColumnNameColIdx].ToString();
            string columnType = dataRow[DataTypeNameColIdx].ToString();
            string columnSize = dataRow[ColumnSizeColIdx].ToString();

            string columnTypeSized = string.Empty;

            if (columnType.Equals("nvarchar"))
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
        //Log.Debug(Fields.ToString());
        return Fields;
    }
}
