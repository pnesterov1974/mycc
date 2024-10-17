using System.Data;
using static Shared.MyLogger;

namespace Datamech
{
    public class DbStruRow
    {
        public int ColumnOrdinal { get; set; } = 0;
        public int ColumnSize { get; set; } = 0;
        public int NumericPrecision { get; set; } = 0;
        public int NumericScale { get; set; } = 0;
        public string DataTypeName { get; set; } = string.Empty;
    }

    public class DbStru
    {
        public Dictionary<string, DbStruRow> Rows { get; set; } = new Dictionary<string, DbStruRow>();
        public DbStru(DataTable sourceDataTable)
        {
            int ColumnNameColIdx = sourceDataTable.Columns["ColumnName"].Ordinal;
            int ColumnOrdinalColIdx = sourceDataTable.Columns["ColumnOrdinal"].Ordinal;
            int ColumnSizeColIdx = sourceDataTable.Columns["ColumnSize"].Ordinal;
            int NumericPrecisionColIdx = sourceDataTable.Columns["NumericPrecision"].Ordinal;
            int NumericScaleColIdx = sourceDataTable.Columns["NumericScale"].Ordinal;
            int DataTypeNameColIdx = sourceDataTable.Columns["DataTypeName"].Ordinal;

            foreach (DataRow dataRow in sourceDataTable.Rows)
            {
                string KeyColumnName = dataRow[ColumnNameColIdx].ToString();

                DbStruRow dbr = new DbStruRow
                {
                    ColumnOrdinal = Convert.ToInt32(dataRow[ColumnOrdinalColIdx]),
                    ColumnSize = Convert.ToInt32(dataRow[ColumnSizeColIdx]),
                    NumericPrecision = Convert.ToInt32(dataRow[NumericPrecisionColIdx]),
                    NumericScale = Convert.ToInt32(dataRow[NumericScaleColIdx]),
                    DataTypeName = dataRow[DataTypeNameColIdx].ToString()
                };
                Log.Information(
                    "ColumnName: {0} => ColumnOrdinal: {1}, ColumnSize: {2}, NumericPrecision: {3}, NumericScale: {4}, DataTypeName: {5}",
                    KeyColumnName, dbr.ColumnOrdinal, dbr.ColumnSize, dbr.NumericPrecision, dbr.NumericScale, dbr.DataTypeName
                );
                this.Rows.Add(KeyColumnName, dbr);
            }
        }
    }

}
