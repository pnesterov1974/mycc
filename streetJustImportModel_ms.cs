using Microsoft.Data.SqlClient;
using Shared.connections;
using static Shared.MyLogger;

namespace Datareader.mssql.KladrJustImportedModels;

public class StreetJustImportedModel : ReadModelBase, IReadModel
{
    public override string ModelName { get; set; } = "Street_justImported";
    public override string SourceDbName { get; set; } = "kladrRaw";
    //public override List<string> SourceKeyFilelds { get; set; } = new List<string>() {"[KladrCode]"};
    public override string SourceSql { get; set; } = """
            SELECT TOP (1000) [NAME],
                        [SOCR],
                        [CODE],
                        [INDEX],
                        [GNINMB],
                        [UNO],
                        [OCATD]
            FROM kladr.[STREET];
            """;
}

public class StreetJustImportedEntityModel
{
        public string NAME { get; set; } = string.Empty;
        public string SOCR { get; set; } = string.Empty;
        public string CODE { get; set; } = string.Empty;
        public string INDEX { get; set; } = string.Empty;
        public string GNINMB { get; set; } = string.Empty;
        public string UNO { get; set; } = string.Empty;
        public string OCATD { get; set; } = string.Empty;
        public override string ToString()
        {
            return $"NAME:{this.NAME} - SOCR:{this.SOCR} - CODE:{this.CODE} - INDEX:{this.INDEX} - GNINMB:{this.GNINMB} - UNO:{this.UNO} - OCATD:{this.OCATD}";
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
        using (SqlConnection conn = new SqlConnection(connString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand(this.ReadModel.SourceSql, conn);
            using SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new StreetJustImportedEntityModel
                    {
                        NAME = reader.GetString(0),
                        SOCR = reader.GetString(1),
                        CODE = reader.GetString(2),
                        INDEX = reader.GetString(3),
                        GNINMB = reader.GetString(4),
                        UNO = reader.GetString(5),
                        OCATD = reader.GetString(6)
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
