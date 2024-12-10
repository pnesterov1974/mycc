using Datareader.pgsql.KladrJustImportedModels;

namespace Datareader.pgsql;
public static class Analizator
{
    public static void Analize1()
    {
        var mk = new KladrModelEnumerator();

        var selectedKladr = from k in mk.GetEnumerator()
                            where k.status == 0
                            select k;
        foreach (var sk in selectedKladr)
        {
            Console.WriteLine(sk);
        }

        var sb = new SocrBaseModelEnumerator();

        var selectedSocrBase = sb.GetEnumerator().Select(s => s.scname);

        foreach (var s in selectedSocrBase)
        {
            Console.WriteLine(s);
        }
    }
}
