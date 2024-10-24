using System.Text;
using static Shared.MyLogger;

namespace Datamech.mssql
{
    public class Sqls
    {
        public RunModel parentModel { get; set; }
        private Source source
        {
            get => this.parentModel.Source;
        }
        private Target target
        {
            get => this.parentModel.Target;
        }

        public Sqls(RunModel parentModel)
        {
            this.parentModel = parentModel;
        }

        public string GetCreateTableSql()
        {
            Log.Information("Получение запроса на создание табл...");
            List<string> sqlRows = new List<string>();
            foreach (var kv in this.source.Dbs.Rows)
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

            foreach (var k in this.source.Dbs.Rows.Keys)
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

        public string GetSelectPageSql(int pageNum, int pageRecordSize = 10000)
        {
            int offsetRecords = (pageNum - 1) * pageRecordSize;
            string strKeyFields = string.Join(", ", this.source.KeyFields);
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("WITH [Data] AS (");
            sb.AppendLine(this.source.SelectSql);
            sb.AppendLine(")");
            sb.AppendLine("SELECT * FROM [Data]");
            sb.AppendLine(string.Concat("ORDER BY ", strKeyFields));
            sb.AppendLine($"OFFSET {offsetRecords} ROWS FETCH NEXT {pageRecordSize} ROWS ONLY;");
            return sb.ToString();
        }

        public string GetMergeSql()
        {
            List<string> mergeKeyList = new List<string>();
            foreach(var s in this.source.KeyFields)
            {
                mergeKeyList.Add($"(Target.{this.TouchMsSqlName(s)} = Source.{this.TouchMsSqlName(s)})");
            }
            string mergeKeyString = string.Concat("ON (", string.Join("\n AND ", mergeKeyList), ")");

            List<string> updateFieldNamesList = new List<string>();
            List<string> insertFieldNamesList = new List<string>();
            int i = 0;
            foreach (var k in this.source.Dbs.Rows.Keys)
            {
                i+=1;
                if (i==1)
                {
                    updateFieldNamesList.Add($"{this.TouchMsSqlName(k)} = Source.{this.TouchMsSqlName(k)}");
                }
                else
                {
                    updateFieldNamesList.Add($"      {this.TouchMsSqlName(k)} = Source.{this.TouchMsSqlName(k)}");
                }
                
                insertFieldNamesList.Add($"Source.{this.TouchMsSqlName(k)}");
            }
            string updateFieldsString = string.Concat("      SET ", string.Join(",\n", updateFieldNamesList));
            string insertFieldsString = string.Join(", ", insertFieldNamesList);

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("DECLARE @SummaryOfChanges TABLE([Change] NVARCHAR(20));\n");
            sb.AppendLine("WITH [Data] AS (");
            sb.AppendLine(this.source.SelectSql);
            sb.AppendLine(")");
            sb.AppendLine($"MERGE {this.target.FullName} AS Target");
            sb.AppendLine("USING [Data] AS Source");
            sb.AppendLine(mergeKeyString);
            sb.AppendLine("WHEN MATCHED");
            sb.AppendLine("    THEN UPDATE");
            sb.AppendLine(updateFieldsString);
            sb.AppendLine("WHEN NOT MATCHED");
            sb.AppendLine("    THEN INSERT VALUES (");
            sb.AppendLine(insertFieldsString);
            sb.AppendLine(")");
            sb.AppendLine("WHEN NOT MATCHED BY SOURCE");
            sb.AppendLine("    THEN DELETE");
            sb.AppendLine("OUTPUT deleted.*, $action, inserted.*;");
            return sb.ToString();
        }
/*
When Matched — описывает действие, которое срабатывает для строк, которые нашлись и в Source, и в Target по условию, которое описано в ON. В этой части чаще всего встречается оператор UPDATE, хотя возможно использование оператора DELETE.
When Not Matched [By Target] — описывает действие для строк, которые есть в таблице Source, но отсутствуют в таблице Target; далее используется оператор INSERT, и указанные строки добавляются в таблицу Target. 
When Not Matched By Source — описывает действие для строк, которые отсутствуют в таблице Source, но найдены в таблице Target, чаще всего встречается оператор DELETE, чтобы удалить строки и привести 2 набора в соответствие, но возможно использование оператора Update.
*/
    }
}
