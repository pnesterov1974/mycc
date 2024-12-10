using Npgsql;
using Shared.connections;
using static Shared.MyLogger;

namespace Datareader.pgsql.KladrJustImportedModels;

public class DomaJustImportedModel : ReadModelBase, IReadModel
{
        public override string ModelName { get; set; } = "Doma_justImported";
        public override string SourceDbName { get; set; } = "kladrRaw";
        //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
        public override string SourceSql { get; set; } = """
            SELECT name,
                   korp,
                   socr,
                   code,
                   index,
                   gninmb,
                   uno,
                   ocatd
            FROM kladr.doma
            """;
}

public class DomaJustImportedEntityModel
{
        public string name { get; set; } = string.Empty;
        public string korp { get; set; } = string.Empty;
        public string socr { get; set; } = string.Empty;
        public string code { get; set; } = string.Empty;
        public string index { get; set; } = string.Empty;
        public string gninmb { get; set; } = string.Empty;
        public string uno { get; set; } = string.Empty;
        public string ocatd { get; set; } = string.Empty;
        public override string ToString()
        {
            return $"NAME:{this.name} - KORP:{this.korp} - SOCR:{this.socr} - CODE:{this.code} - INDEX:{this.index} - GNINMB:{this.gninmb} - UNO:{this.uno} - OCATD:{this.ocatd}";
        }
}

public class DomaModelEnumerator
{
    public IReadModel ReadModel = new DomaJustImportedModel();
    public DomaJustImportedEntityModel EntityModel = new DomaJustImportedEntityModel();

    public IEnumerable<DomaJustImportedEntityModel> GetEnumerator()
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
                    var em = new DomaJustImportedEntityModel
                    {
                        name = reader.GetString(0),
                        korp = reader.GetString(1),
                        socr = reader.GetString(2),
                        code = reader.GetString(3),
                        index = reader.GetString(4),
                        gninmb = reader.GetString(5),
                        uno = reader.GetString(6),
                        ocatd = reader.GetString(7)
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


