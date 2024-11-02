using System.Data;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;
using Shared;
using Shared.connections;
using Newtonsoft.Json;

namespace Dataread.mssql
{
    public class RunModel
    {
        private IReadModel readModel;
        public IReadModel ReadModel
        {
            get
            {
                if (this.readModel != null)
                {
                    return this.readModel;
                }
                else
                {
                    throw new NullReferenceException("RunModel: etlModel не инициализирован");
                }
            }
        }

        private DataTable? data;
        public DataTable Data
        {
            get
            {
                if (this.data != null)
                {
                    return this.data;
                }
                else
                {
                    this.Read();
                    if (this.data != null)
                    { 
                        return this.data;
                    }
                    else
                    {
                        throw new NullReferenceException("ReadModel: data не инициализирован");
                    }
                }
            }
        }
        private DataTable? schemaData;
        public DataTable SchemaData
        {
            get
            {
                if (this.schemaData != null)
                {
                    return this.schemaData;
                }
                else
                {
                    this.Read();
                    if (this.schemaData != null)
                    { 
                        return this.schemaData;
                    }
                    else
                    {
                        throw new NullReferenceException("ReadModel: schemaData не инициализирован");
                    }
                }
            }
        }
        public string ConnectionString { get; set; } = string.Empty;

        private bool readModelIsValid = false;

        public RunModel(IReadModel readModel)
        {
            if (readModel == null)
            {
                throw new ArgumentNullException("readModelSource.sourceDbStru: в конструктор Source передан parentModel: null");
            }
            else
            {
                try
                {
                    this.readModel = readModel;
                    ValidateModel vm = new ValidateModel(TargetDb.mssql, this.readModel);
                    this.readModelIsValid = vm.Validate();

                    if (this.readModelIsValid)
                    {
                        this.ConnectionString = ConnectionStrings.GetConnectionStringByName(this.readModel.SourceDbName);
                    }
                    else
                    {
                        ;
                    }
                }
                catch
                {
                    throw;
                }
            }
        }

        public void Read()
        {
            if (this.readModelIsValid)
            {
                Log.Information("Загрузка данных...");
                Log.Debug("Загрузка данных из : \n{connstr}", this.ConnectionString);
                Log.Debug("SQL : \n{connstr}", this.readModel.SourceSql);
                this.data = new DataTable();
                int RecordCount = 0;
                using (SqlConnection conn = new SqlConnection(this.ConnectionString))
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(this.readModel.SourceSql, conn);
                    try
                    {
                        RecordCount = adapter.Fill(this.Data);
                        Log.Information("Загружено {recorcount} записей", RecordCount);
                    }
                    catch (Exception ex)
                    {
                        Log.Debug("Ошибка чтения даннтых \n {errmes}", ex.Message);
                    }
                }
            }
        }

        public string DataAsJson
        {
            get
            {
                if (this.Data == null)
                {
                    this.Read(); // reread data
                }
                return JsonConvert.SerializeObject(this.Data, Formatting.Indented);
            }
        }

        public void ReadSchema()
        {
            Log.Information("Загрузка схемы...");
            Log.Debug("Загрузка схемы из : \n{connstr}", this.ConnectionString);
            Log.Debug("SQL : \n{connstr}", this.readModel.SourceSql);
            this.data = new DataTable();

            using (SqlConnection conn = new SqlConnection(this.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(this.readModel.SourceSql, conn))
                {
                    try
                    {
                        conn.Open();
                        using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                        {
                            this.schemaData = rdr.GetSchemaTable();
                            Log.Information("Схема загружена");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Debug("Ошибка чтения даннтых \n {errmes}", ex.Message);
                    }
                }
            }
        }

        public string SchemaAsJson
        {
            get
            {
                if (this.schemaData == null)
                {
                    this.ReadSchema(); // reread data
                }
                return JsonConvert.SerializeObject(this.schemaData, Formatting.Indented);
            }
        }

        public Dictionary<string, string> GetFieldList()
        {
            int ColumnNameColIdx = this.Data.Columns["BaseColumnName"].Ordinal;
            int DataTypeNameColIdx = this.Data.Columns["DataTypeName"].Ordinal;
            int ColumnSizeColIdx = this.Data.Columns["ColumnSize"].Ordinal;

            Dictionary<string, string> Fields = new Dictionary<string, string>();

            Log.Information("first: {col} second: {type}", ColumnNameColIdx, DataTypeNameColIdx);
            foreach (DataRow dataRow in this.Data.Rows)
            {
                string columnName = dataRow[ColumnNameColIdx].ToString();
                string columnType = dataRow[DataTypeNameColIdx].ToString();
                string columnSize = dataRow[ColumnSizeColIdx].ToString();

                string columnTypeSized = string.Empty;

                if (columnType.Equals("nvarchar"))
                {
                    columnTypeSized = $"{columnType}({columnSize})";
                }
                else
                {
                    columnTypeSized = columnType;
                }
                Fields.Add(columnName, columnTypeSized);
                Log.Debug("columnName: {col} columnType: {type}", columnName, columnTypeSized);
            }
            //Log.Debug(Fields.ToString());
            return Fields;
        }
    }
}
