using Datareader.mssql.KladrJustImportedModels;

namespace Datareader.mssql;
public static class Analizator
{
    public static void Analize1()
    {
        var mk = new KladrModelEnumerator();

        var selectedKladr = from k in mk.GetEnumerator()
                            where k.STATUS == 0
                            select k;
        foreach (var sk in selectedKladr)
        {
            Console.WriteLine(sk);
        }

        var sb = new SocrBaseModelEnumerator();

        var selectedSocrBase = sb.GetEnumerator().Select(s => s.SCNAME);

        foreach (var s in selectedSocrBase)
        {
            Console.WriteLine(s);
        }
    }
}
