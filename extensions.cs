namespace Datamech.mssql
{
    public static class Extensions
    {
        public static string TouchMsSqlName(this Sqls sqls, string mssqlName)
        {
            return touchMsSqlName(mssqlName);
        }

        public static string ClearQBracketsInMsSqlName(this Sqls sqls, string mssqlName)
        {
            return clearQBracketsInMsSqlName(mssqlName);
        }

        private static string touchMsSqlName(string mssqlName)
        {
            //return string.Concat('[', mssqlName.Replace("[", string.Empty).Replace("]", string.Empty), ']');
            return string.Concat('[', Extensions.clearQBracketsInMsSqlName(mssqlName), ']');
        }

        private static string clearQBracketsInMsSqlName(string mssqlName)
        {
            return mssqlName.Replace("[", string.Empty).Replace("]", string.Empty);
            //return string.Concat('[', mssqlName.Replace("[", string.Empty).Replace("]", string.Empty), ']');
        }
    }
}
