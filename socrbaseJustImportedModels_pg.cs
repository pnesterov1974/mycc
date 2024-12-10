using Npgsql;
using Shared.connections;
using static Shared.MyLogger;

namespace Datareader.pgsql.KladrJustImportedModels;

public class SocrBaseJustImportedModel : ReadModelBase, IReadModel
{
    public override string ModelName { get; set; } = "SocrBase_justImported";
    public override string SourceDbName { get; set; } = "kladrRaw";
    //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
    public override string SourceSql { get; set; } = """
            SELECT level,
                   scname,
                   socrname,
                   kod_t_st
            FROM kladr.socrbase
            """;
}

public class SocrBaseJustImportedEntityModel
{
    public int level { get; set; }
    public string scname { get; set; } = string.Empty;
    public string socrname { get; set; } = string.Empty;
    public string kod_t_st { get; set; } = string.Empty;
    public override string ToString()
    {
        return $"LEVEL:{this.level} - SCNAME:{this.scname} - SOCRNAME:{this.socrname} - KOD_T_ST:{this.kod_t_st}";
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
        using (NpgsqlConnection conn = new NpgsqlConnection(connString))
        {
            conn.Open();
            NpgsqlCommand command = new NpgsqlCommand(this.ReadModel.SourceSql, conn);
            using NpgsqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new SocrBaseJustImportedEntityModel
                    {
                        level = (int)reader.GetInt16(0),
                        scname = reader.GetString(1),
                        socrname = reader.GetString(2),
                        kod_t_st = reader.GetString(3)
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
