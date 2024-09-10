using System.Xml;
using Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;

namespace Import.gar.mssql;
public class ImportAddHouseTypes: BasePgSql
{
    protected ImportObjectInfo objectInfo { get; set; }
    public ImportAddHouseTypes(ImportObjectInfo objectInfo) => this.objectInfo = objectInfo;

    private List<AddHouseTypes> GetSourceData()
    {
        List<AddHouseTypes> res = new List<AddHouseTypes>();
        Log.Information("Импорт из файла ...{file}", this.objectInfo.SourceFileName);
        XmlDocument xDoc = new XmlDocument();
        xDoc.Load(this.objectInfo.SourceFilePath);
        XmlElement? xRoot = xDoc.DocumentElement;
        if (xRoot != null)
        {
            foreach (XmlElement xnode in xRoot)
            {
                XmlNode idAttr = xnode.Attributes.GetNamedItem("ID");
                int id = int.Parse(idAttr.Value);
                XmlNode NameAttr = xnode.Attributes.GetNamedItem("NAME");
                string name = NameAttr.Value;
                XmlNode? ShortNameAttr = xnode.Attributes.GetNamedItem("SHORTNAME");
                string? shortname = ShortNameAttr?.Value;
                XmlNode? DescAttr = xnode.Attributes.GetNamedItem("DESC");
                string? desc = DescAttr?.Value;
                XmlNode IsActiveAttr = xnode.Attributes.GetNamedItem("ISACTIVE");
                string _isActive = IsActiveAttr.Value;
                bool isActive = false;
                if (_isActive.ToLower().Equals("true"))
                {
                    isActive = true;
                }
                else if (_isActive.ToLower().Equals("false"))
                {
                    isActive = false;
                }
                XmlNode UpdateDateAttr = xnode.Attributes.GetNamedItem("UPDATEDATE");
                DateOnly updateDate = DateOnly.Parse(UpdateDateAttr.Value);
                XmlNode StartDateAttr = xnode.Attributes.GetNamedItem("STARTDATE");
                DateOnly startDate = DateOnly.Parse(StartDateAttr.Value);
                XmlNode EndDateAttr = xnode.Attributes.GetNamedItem("ENDDATE");
                DateOnly endDate = DateOnly.Parse(EndDateAttr.Value);
                res.Add(new AddHouseTypes
                {
                    Id = id,
                    Name = name,
                    ShortName = shortname,
                    Desc = desc,
                    IsActive = isActive,
                    UpdateDate = updateDate,
                    StartDate = startDate,
                    EndDate = endDate
                }
                );
                Log.Information("ID:{id} == NAME:{name} == SHORTNAME:{shortname} == DESC:{desc} == ISACTIVE:{isactive} == UPDATEDATE:{updatedate} == STARTDATE:{startdate} == ENDDATE:{enddate}", id, name, shortname, desc, isActive, updateDate, startDate, endDate);
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
                var data = this.GetSourceData();

                using var conn = new SqlConnection(this.objectInfo.ConnectionString);
                conn.Open();

                var batch = new SqlBatch(conn);

                foreach (var d in data)
                {
                    var bcmd = new SqlBatchCommand(
                        $"INSERT INTO {this.objectInfo.TargetTableFullName}([Id], [Name], [ShortName], [Descr], [IsActive], [UpdateDate], [StartDate], [EndDate]) VALUES (@Id, @Name, @ShortName, @Desc, @IsActive, @UpdateDate, @StartDate, @EndDate);"
                        );

                    bcmd.Parameters.AddWithValue("@Id", d.Id);
                    bcmd.Parameters.AddWithValue("@Name", d.Name);
                    bcmd.Parameters.AddWithValue("@ShortName", d.ShortName);
                    bcmd.Parameters.AddWithValue("@Desc", d.Desc);
                    bcmd.Parameters.AddWithValue("@IsActive", d.IsActive);
                    bcmd.Parameters.AddWithValue("@UpdateDate", d.UpdateDate);
                    bcmd.Parameters.AddWithValue("@StartDate", d.StartDate);
                    bcmd.Parameters.AddWithValue("@EndDate", d.EndDate);

                    Log.Debug(bcmd.CommandText);

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
