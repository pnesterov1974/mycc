namespace import_kladr_efc;

public static class FirstDBRun
{
    public static void KladrData()
    {
        using (KladrContext ctx = new KladrContext())
        {
            bool isAvailaible = ctx.Database.CanConnect();
            if (isAvailaible)
            {
                Console.WriteLine("Подключаюсь...");
                var kladrs = ctx.Kladrs.ToList();
                foreach(var kladr in kladrs)
                {
                    Console.WriteLine($"{kladr.NAME} {kladr.SOCR} {kladr.CODE} {kladr.INDEX} {kladr.GNINMB} {kladr.UNO} {kladr.OCATD} {kladr.STATUS}");
                }
            }
            else
            {
                Console.WriteLine("Подулючение недоступно...");
            }
        }
    }

    public void SaveKladr()
    {
        using (KladrContext ctx = new KladrContext())
        {
            Kladr kl1 = new Kladr
            {
               NAME = "1",
               SOCR = "1",
               CODE = "1",
               INDEX = "1",
               GNINMB = "1",
               UNO = "1",
               OCATD = "1",
               STATUS = 1
            };

            Kladr kl2 = new Kladr
            {
               NAME = "2",
               SOCR = "2",
               CODE = "2",
               INDEX = "2",
               GNINMB = "2",
               UNO = "2",
               OCATD = "2",
               STATUS = 1
            };
        }
    }
}
