using Microsoft.Data.SqlClient;
using Shared.connections;
using static Shared.MyLogger;

namespace Datareader.mssql.KladrJustImportedModels;

public class SocrBaseJustImportedModel : ReadModelBase, IReadModel
{
    public override string ModelName { get; set; } = "SocrBase_justImported";
    public override string SourceDbName { get; set; } = "kladrRaw";
    //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
    public override string SourceSql { get; set; } = """
            SELECT TOP (1000) 
                   [LEVEL],
                   [SCNAME],
                   [SOCRNAME],
                   [KOD_T_ST]
            FROM kladr.[SOCRBASE]
            """;
}

public class SocrBaseJustImportedEntityModel
{
    public int LEVEL { get; set; }
    public string SCNAME { get; set; } = string.Empty;
    public string SOCRNAME { get; set; } = string.Empty;
    public string KOD_T_ST { get; set; } = string.Empty;
    public override string ToString()
    {
        return $"LEVEL:{this.LEVEL} - SCNAME:{this.SCNAME} - SOCRNAME:{this.SOCRNAME} - KOD_T_ST:{this.KOD_T_ST}";
    }
}

public class SocrBaseModelEnumerator
{
    public IReadModel ReadModel = new SocrBaseJustImportedModel();
    public SocrBaseJustImportedEntityModel EntityModel = new SocrBaseJustImportedEntityModel();

    public IEnumerable<SocrBaseJustImportedEntityModel> GetEnumerator()
    {
        Log.Information("Загрузка данных...");
        string connString = ConnectionStrings.GetConnectionStringByName("kladrStage");
        Log.Debug("Загрузка данных из : \n{connString}", connString);
        Log.Debug("SQL : \n{connstr}", this.ReadModel.SourceSql);
        using (SqlConnection conn = new SqlConnection(connString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand(this.ReadModel.SourceSql, conn);
            using SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new SocrBaseJustImportedEntityModel
                    {
                        LEVEL = (int)reader.GetInt16(0),
                        SCNAME = reader.GetString(1),
                        SOCRNAME = reader.GetString(2),
                        KOD_T_ST = reader.GetString(3)
                    };
                    yield return em;
                }
            }
            else
            {
                Log.Information("Данные отсутствуют...");
                yield break;
            }
        }
    }

}
