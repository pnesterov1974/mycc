using Microsoft.Data.SqlClient;
using Npgsql;
using Shared.connections;
using static Shared.MyLogger;

namespace Datareader.pgsql.KladrJustImportedModels;

public class StreetJustImportedModel : ReadModelBase, IReadModel
{
    public override string ModelName { get; set; } = "Street_justImported";
    public override string SourceDbName { get; set; } = "kladrRaw";
    //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
    public override string SourceSql { get; set; } = """
            SELECT name,
                   socr,
                   code,
                   index,
                   gninmb,
                   uno,
                   ocatd
            FROM kladr.street;
            """;
}

public class StreetJustImportedEntityModel
{
        public string name { get; set; } = string.Empty;
        public string socr { get; set; } = string.Empty;
        public string code { get; set; } = string.Empty;
        public string index { get; set; } = string.Empty;
        public string gninmb { get; set; } = string.Empty;
        public string uno { get; set; } = string.Empty;
        public string ocatd { get; set; } = string.Empty;
        public override string ToString()
        {
            return $"NAME:{this.name} - SOCR:{this.socr} - CODE:{this.code} - INDEX:{this.index} - GNINMB:{this.gninmb} - UNO:{this.uno} - OCATD:{this.ocatd}";
        }
}

public class StreetModelEnumerator
{
    public IReadModel ReadModel = new StreetJustImportedModel();
    public StreetJustImportedEntityModel EntityModel = new StreetJustImportedEntityModel();

    public IEnumerable<StreetJustImportedEntityModel> GetEnumerator()
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
                    var em = new StreetJustImportedEntityModel
                    {
                        name = reader.GetString(0),
                        socr = reader.GetString(1),
                        code = reader.GetString(2),
                        index = reader.GetString(3),
                        gninmb = reader.GetString(4),
                        uno = reader.GetString(5),
                        ocatd = reader.GetString(6)
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
