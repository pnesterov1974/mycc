using System.Data;
using System.Text;
//using Newtonsoft.Json;
using static Shared.MyLogger;

namespace Datamech.mssql
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

    public class Sqls
    {
        private Source source { get; set; }
        private Target target { get; set; }
        public DbStru Dbs { get; set; }
        public Sqls(Source src, Target trg)
        {
            this.source = src;
            this.target = trg;
            this.Dbs = new DbStru(this.source.SourceDbStru);
        }

        //public void SerializeToJson(string filePath)
        //{
        //   string serialized = JsonConvert.SerializeObject(this.Dbs, Formatting.Indented);
        //    File.WriteAllText(filePath, serialized);
        //}

        public string GetCreateTableSql()
        {
            List<string> sqlRows = new List<string>();
            foreach (var kv in this.Dbs.Rows)
            {
                string colName = kv.Key;
                DbStruRow dbr = kv.Value;
                string colType;
                if ((dbr.DataTypeName == "varchar") || (dbr.DataTypeName == "nvarchar"))
                {
                    colType = string.Concat(dbr.DataTypeName, '(', dbr.ColumnSize, ')');
                }
                else
                {
                    colType = dbr.DataTypeName;
                }
                string item = string.Concat(this.TouchMsSqlName(colName), ' ', colType, ' ', "null"); // null vs not null
                sqlRows.Add(item);
            }
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {this.target.FullName} (");
            sb.AppendLine(string.Join(",\n", sqlRows));
            sb.AppendLine(");\n");
            return sb.ToString();
        }

        public string GetInsertIntoValuesSql()
        {
            List<string> cols = new List<string>();
            List<string> paramValues = new List<string>();

            foreach (var k in this.Dbs.Rows.Keys)
            {
                cols.Add(this.TouchMsSqlName(k));
                paramValues.Add(string.Concat("@", k));
            }

            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"INSERT INTO {this.target.FullName} (");
            sb.AppendLine(string.Join(", ", cols));
            sb.AppendLine(") VALUES (");
            sb.AppendLine(string.Join(", ", paramValues));
            sb.AppendLine(");");
            return sb.ToString();
        }

        public string GetInsertIntoSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"INSERT INTO {this.target.FullName}");
            sb.AppendLine(this.source.SelectSql);
            sb.AppendLine("\n");
            return sb.ToString();
        }

        public string GetDropTableSql()
        {
            string fullTargetName = string.Concat(this.target.SchemaName, '.', this.TouchMsSqlName(this.target.TableName));
            return $"DROP TABLE {fullTargetName};";
        }

        public string GetIsObjectExistsSql()
        {
            string fullTargetName = string.Concat(this.target.SchemaName, '.', this.TouchMsSqlName(this.target.TableName));
            return $"""
            IF OBJECT_ID(N'{fullTargetName}', N'U') IS NOT NULL
                SELECT 1 AS res
            ELSE SELECT 0 AS res;
            """;
        }

        public string GetSelectBatchSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("WITH [Data] AS (");
            string strKeyFields = string.Join(", ", this.source.KeyFields);
            sb.AppendLine($"SELECT [rnn] = ROW_NUMBER() OVER (ORDER BY {strKeyFields}),");
            sb.AppendLine("        q.*");
            sb.AppendLine("FROM (");
            sb.AppendLine(this.source.SelectSql);
            sb.AppendLine($"      ) q");
            sb.AppendLine(")");
            sb.AppendLine("SELECT * FROM [Data]");
            sb.AppendLine("WHERE [rnn] BETWEEN @rnnFrom AND @rnnTo;");
            return sb.ToString();
        }

        public string GetSelectBatchSqlBouded(int rnnFrom, int rnnTo)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("WITH [Data] AS (");
            string strKeyFields = string.Join(", ", this.source.KeyFields);
            sb.AppendLine($"SELECT [rnn] = ROW_NUMBER() OVER (ORDER BY {strKeyFields}),");
            sb.AppendLine("        q.*");
            sb.AppendLine("FROM (");
            sb.AppendLine(this.source.SelectSql);
            sb.AppendLine($"      ) q");
            sb.AppendLine(")");
            sb.AppendLine("SELECT * FROM [Data]");
            sb.AppendLine($"WHERE [rnn] BETWEEN {rnnFrom} AND {rnnTo};");
            return sb.ToString();
        }
    }
}
