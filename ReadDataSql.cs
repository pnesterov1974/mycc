using System.Data;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using static Shared.MyLogger;

namespace Data.mesh.mssql;

public class MsSqlData
{
    public DataTable? Data { get; set; } = null;
    protected string connectionString{ get; set; } = string.Empty;
    protected string selectSql{ get; set; } = string.Empty;

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

    public MsSqlData(string connectionString, string selectSql)
    {
        this.connectionString = connectionString;
        this.selectSql = selectSql;
    }
    
    public void Read()
    {
        Log.Information("Загрузка данных...");
        Log.Debug("Загрузка данных из : \n{connstr}", this.connectionString);
        Log.Debug("SQL : \n{connstr}", this.selectSql);
        this.Data = new DataTable();
        int RecordCount = 0;
        using (SqlConnection conn = new SqlConnection(this.connectionString))
        {
            SqlDataAdapter adapter = new SqlDataAdapter(this.selectSql, conn);
            try
            {
                RecordCount = adapter.Fill(this.Data);
                Log.Information("Загружено {recorcount} записей", RecordCount);
            }
            catch (Exception ex)
            {
                Log.Debug("Ошибка чтения даннтых \n {errmes}", ex.Message);
            }
        }
    }
}
