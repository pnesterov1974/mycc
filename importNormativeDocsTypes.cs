using System.Xml;
using Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;
using xmlUtils2;
using Data.Import.gar;

namespace Import.gar.mssql;
public class ImportNormativeDocsTypes : BaseMsSql
{
    protected ImportObjectInfo objectInfo { get; set; }
    public ImportNormativeDocsTypes(ImportObjectInfo objectInfo) => this.objectInfo = objectInfo;

    private List<NormativeDocsTypes> GetSourceData(out int ErrorsCount)
    {
        ErrorsCount = 0;
        List<NormativeDocsTypes> res = new List<NormativeDocsTypes>();
        Log.Information("Импорт из файла ...{file}", this.objectInfo.SourceFileName);
        XmlDocument xDoc = new XmlDocument();
        xDoc.Load(this.objectInfo.SourceFilePath);
        XmlElement? xRoot = xDoc.DocumentElement;
        if (xRoot != null)
        {
            Log.Information("Найдены элементы");
            foreach (XmlElement xnode in xRoot)
            {
                int id = XMLUtils.GetIntNotNullNodeValue(xnode, "ID", this.objectInfo.SourceFileName, "ключевое поле", ref ErrorsCount);
                string name = XMLUtils.GetStringNotNullNodeValue(xnode, "NAME", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                DateOnly startDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "STARTDATE", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                DateOnly endDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "ENDDATE", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                
                res.Add(new NormativeDocsTypes
                {
                    Id = id,
                    Name = name,
                    StartDate = startDate,
                    EndDate = endDate
                });
            }
        }
        else
        {
            Log.Information("Элементы не найдены");
        }
        return res;
    }

    public void DoImport(bool clearDestTableInAdvance = true)
    {
        Log.Information("Импорт из файла {file}", this.objectInfo.SourceFilePath);

        if (File.Exists(this.objectInfo.SourceFilePath))
        {
            Log.Information("Файл {file} существует...", this.objectInfo.SourceFileName);
            if (!clearDestTableInAdvance ||
                (clearDestTableInAdvance &&
                    this.ClearDestTable(
                        this.objectInfo.ConnectionString,
                        this.objectInfo.TargetTableFullName
                    )
                )
           )
            {
                int ErrorCount;
                var data = this.GetSourceData(out ErrorCount);

                using var conn = new SqlConnection(this.objectInfo.ConnectionString);
                conn.Open();

                var batch = new SqlBatch(conn);

                foreach (var d in data)
                {
                    var bcmd = new SqlBatchCommand($"""
                        INSERT INTO {this.objectInfo.TargetTableFullName}([Id], [Name], [StartDate], [EndDate]) 
                        VALUES (@Id, @Name, @StartDate, @EndDate);
                    """);
                    bcmd.Parameters.AddWithValue("@Id", d.Id);
                    bcmd.Parameters.AddWithValue("@Name", d.Name);
                    bcmd.Parameters.AddWithValue("@StartDate", d.StartDate);
                    bcmd.Parameters.AddWithValue("@EndDate", d.EndDate);

                    batch.BatchCommands.Add(bcmd);
                }

                int recs = batch.ExecuteNonQuery();
                Log.Information("Загружено записей {recs} из {file}. Ошибок конвертации {errs}", recs, this.objectInfo.SourceFileName, ErrorCount);
            }
        }
        else
        {
            Log.Information("Файл {file} не найден", this.objectInfo.SourceFilePath);
        }
    }
}
