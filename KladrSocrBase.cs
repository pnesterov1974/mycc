public class SocrBase
{
    public int Id;
    public int Level;
    public string ScName;
    public string SocrName;
    public string Kod_T_St;
    public DateOnly BusinessDT;
}

public static class SocrBaseClass
{
    public static string SelectSql = """
        SELECT [Id],
            [Level],
            [ScName],
            [SocrName],
            [KodTSt],
            [BusinessDT]
        FROM [kladrStg].stg.[SocrBase];
    """;

    public static IEnumerable<SocrBase> IterSocrBase(string connString)
    {
        //string connString = MsSqlConnTest.MsSqlConnectionString();
        using (SqlConnection conn = new SqlConnection(connString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand(SocrBaseClass.SelectSql, conn);
            using SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new SocrBase
                    {
                        //Id = (int)reader.GetInt32(0),
                        Id = reader.GetFieldValue<int>(0),
                        //Level = (int)reader.GetInt32(1),
                        Level = reader.GetFieldValue<int>(1),
                        //ScName = reader.GetString(2),
                        ScName = reader.GetFieldValue<string>(2),
                        //SocrName = reader.GetString(3),
                        SocrName = reader.GetFieldValue<string>(3),
                        //Kod_T_St = reader.GetString(4),
                        Kod_T_St = reader.GetFieldValue<string>(4),
                        BusinessDT = reader.GetFieldValue<DateOnly>(5)
                    };
                    yield return em;
                }
            }
            else
            {
                Console.WriteLine("Данные отсутствуют...");
                yield break;
            }
        }
    }
}
