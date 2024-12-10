using static Shared.MyLogger;
using Shared.connections;
using Microsoft.Data.SqlClient;

namespace Datareader.mssql.KladrJustImportedModels;

public class AltNamesJustImportedModel : ReadModelBase, IReadModel
{
    public override string ModelName { get; set; } = "AltNames_justImported";
    public override string SourceDbName { get; set; } = "kladrRaw";
    //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
    public override string SourceSql { get; set; } = """
            SELECT TOP (1000) 
                   [OLDCODE]
                   [NEWCODE]
                   [LEVEL]
            FROM kladr.[ALTNAMES]
            """;
}

public class AltNamesJustImportedEntityModel
{
    public string OLDCODE { get; set; } = string.Empty;
    public string NEWCODE { get; set; } = string.Empty;
    public int LEVEL { get; set; }
    public override string ToString()
    {
        return $"OLDCODE:{this.OLDCODE} - NEWCODE:{this.NEWCODE} - LEVEL:{this.LEVEL}";
    }
}

public class AltNamesModelEnumerator
{
    public IReadModel ReadModel = new AltNamesJustImportedModel();
    public AltNamesJustImportedEntityModel EntityModel = new AltNamesJustImportedEntityModel();

    public IEnumerable<AltNamesJustImportedEntityModel> GetEnumerator()
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
                    var em = new AltNamesJustImportedEntityModel
                    {
                        LEVEL = (int)reader.GetInt16(0),
                        OLDCODE = reader.GetString(1),
                        NEWCODE = reader.GetString(2)
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
