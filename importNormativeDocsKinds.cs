using System.Xml;
using Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;
using xmlUtils;

namespace Import.gar.mssql;
public class ImportNormativeDocsKinds: BaseMsSql
{
    protected ImportObjectInfo objectInfo { get; set; }
    public ImportNormativeDocsKinds(ImportObjectInfo objectInfo) => this.objectInfo = objectInfo;

    private List<NormativeDocsKinds> GetSourceData()
    {
        List<NormativeDocsKinds> res = new List<NormativeDocsKinds>();
        Log.Information("Импорт из файла ...{file}", this.objectInfo.SourceFileName);
        XmlDocument xDoc = new XmlDocument();
        xDoc.Load(this.objectInfo.SourceFilePath);
        XmlElement? xRoot = xDoc.DocumentElement;
        if (xRoot != null)
        {
            Log.Information("Найдены элементы");
            foreach (XmlElement xnode in xRoot)
            {
                int id = XMLUtils.GetIntNotNullNodeValue(xnode, "ID");
                string name = XMLUtils.GetStringNotNullNodeValue(xnode, "NAME");

                res.Add(new NormativeDocsKinds
                {
                    Id = id,
                    Name = name
                }
                );
                //Log.Information("ID: {id} ==== NAME: {name}", id, name);
            }
        }
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
                var data = this.GetSourceData();

                using var conn = new SqlConnection(this.objectInfo.ConnectionString);
                conn.Open();

                var batch = new SqlBatch(conn);

                foreach (var d in data)
                {
                    var bcmd = new SqlBatchCommand($"INSERT INTO {this.objectInfo.TargetTableFullName}([Id], [Name]) VALUES (@Id, @Name);");
                    bcmd.Parameters.AddWithValue("@Id", d.Id);
                    bcmd.Parameters.AddWithValue("@Name", d.Name);
                    batch.BatchCommands.Add(bcmd);
                }

                int recs = batch.ExecuteNonQuery();
                Log.Information("Загружено записей {recs} из {file}", recs, this.objectInfo.SourceFileName);
            }
        }
        else
        {
            Log.Information("Файл {file} не найден", this.objectInfo.SourceFilePath);
        }
    }
}
