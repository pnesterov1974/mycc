namespace Datamech.mssql
{
    public static class Extensions
    {
        public static string TouchMsSqlName(this Sqls sqls, string mssqlName)
        {
            return touchMsSqlName(mssqlName);
        }

        public static string TouchMsSqlName(this Source src, string mssqlName)
        {
            return touchMsSqlName(mssqlName);
        }

        public static string touchMsSqlName(string mssqlName)
        {
            return string.Concat('[', mssqlName.Replace("[", string.Empty).Replace("]", string.Empty), ']');
        }
    }
}
