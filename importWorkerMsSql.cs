namespace Import.kladr.mssql;

public static class DoImportMsSql
{
    public static string SourceDirPath = string.Empty;
    public static string ConnectionString = string.Empty;
    public static List<string> NowInvoking = new List<string>();

    public static void DoImportParallel()
    {
        Parallel.Invoke(
            () =>
                {
                    if (!NowInvoking.Contains("socrbase"))
                    {
                        NowInvoking.Add("socrbase");
                        MsSocrBaseImport sb = new MsSocrBaseImport(SourceDirPath, ConnectionString);
                        sb.DoImport();
                        NowInvoking.Remove("socrbase");
                    }
                },
            () =>
                {
                    if (!NowInvoking.Contains("altnames"))
                    {
                        NowInvoking.Add("altnames");
                        MsAltNamesImport an = new MsAltNamesImport(SourceDirPath, ConnectionString);
                        an.DoImport();
                        NowInvoking.Remove("altnames");
                    }
                },
            () =>
                {
                    if (!NowInvoking.Contains("kladr"))
                    {
                        NowInvoking.Add("kladr");
                        MsKladrImport kl = new MsKladrImport(SourceDirPath, ConnectionString);
                        kl.DoImport();
                        NowInvoking.Remove("kladr");
                    }
                },
            () =>
                {
                    if (!NowInvoking.Contains("street"))
                    {
                        NowInvoking.Add("street");
                        MsStreetImport st = new MsStreetImport(SourceDirPath, ConnectionString);
                        st.DoImport();
                        NowInvoking.Remove("street");
                    }
                },
            () =>
                {
                    if (!NowInvoking.Contains("doma"))
                    {
                        NowInvoking.Add("doma");
                        MsDomaImport dm = new MsDomaImport(SourceDirPath, ConnectionString);
                        dm.DoImport();
                        NowInvoking.Remove("doma");
                    }
                }
        );
    }

    public static void DoImportSynch()
    {
        MsSocrBaseImport sb = new MsSocrBaseImport(SourceDirPath, ConnectionString);
        sb.DoImport();

        MsAltNamesImport an = new MsAltNamesImport(SourceDirPath, ConnectionString);
        an.DoImport();

        MsKladrImport kl = new MsKladrImport(SourceDirPath, ConnectionString);
        kl.DoImport();

        MsStreetImport st = new MsStreetImport(SourceDirPath, ConnectionString);
        st.DoImport();

        MsDomaImport dm = new MsDomaImport(SourceDirPath, ConnectionString);
        dm.DoImport();
    }
}
