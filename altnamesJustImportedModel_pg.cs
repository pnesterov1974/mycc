using static Shared.MyLogger;
using Shared.connections;
using Npgsql;

namespace Datareader.pgsql.KladrJustImportedModels;

public class AltNamesJustImportedModel : ReadModelBase, IReadModel
{
    public override string ModelName { get; set; } = "AltNames_justImported";
    public override string SourceDbName { get; set; } = "kladrRaw";
    //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
    public override string SourceSql { get; set; } = """
            SELECT oldcode,
                   newcode,
                   level
            FROM kladr.altnames
            """;
}

public class AltNamesJustImportedEntityModel
{
    public string oldcode { get; set; } = string.Empty;
    public string newcode { get; set; } = string.Empty;
    public int level { get; set; }
    public override string ToString()
    {
        return $"OLDCODE:{this.oldcode} - NEWCODE:{this.newcode} - LEVEL:{this.level}";
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
        using (NpgsqlConnection conn = new NpgsqlConnection(connString))
        {
            conn.Open();
            NpgsqlCommand command = new NpgsqlCommand(this.ReadModel.SourceSql, conn);
            using NpgsqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new AltNamesJustImportedEntityModel
                    {
                        level = (int)reader.GetInt16(0),
                        oldcode = reader.GetString(1),
                        newcode = reader.GetString(2)
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
