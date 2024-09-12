using System.Xml;
using Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;
using xmlUtils;

namespace Import.gar.mssql;
public class ImportRoomTypes: BaseMsSql
{
    protected ImportObjectInfo objectInfo { get; set; }
    public ImportRoomTypes(ImportObjectInfo objectInfo) => this.objectInfo = objectInfo;

    private List<RoomTypes> GetSourceData()
    {
        List<RoomTypes> res = new List<RoomTypes>();
        Log.Information("Импорт из файла ...{file}", this.objectInfo.SourceFileName);
        XmlDocument xDoc = new XmlDocument();
        xDoc.Load(this.objectInfo.SourceFilePath);
        XmlElement? xRoot = xDoc.DocumentElement;
        if (xRoot != null)
        {
            foreach (XmlElement xnode in xRoot)
            {
                int id = XMLUtils.GetIntNotNullNodeValue(xnode, "ID");
                string name = XMLUtils.GetStringNotNullNodeValue(xnode, "NAME");
                string? shortname = XMLUtils.GetStringNotNullNodeValue(xnode, "SHORTNAME");
                string? desc = XMLUtils.GetStringNullNodeValue(xnode, "DESC");
                bool isactive = (bool)XMLUtils.GetBoolNullNodeValue(xnode, "ISACTIVE");
                DateOnly updateDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "UPDATEDATE");
                DateOnly startDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "STARTDATE");
                DateOnly endDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "ENDDATE");

                res.Add(new RoomTypes
                    {
                        Id = id,
                        Name = name,
                        ShortName = shortname,
                        Desc = desc,
                        IsActive = isactive,
                        UpdateDate = updateDate,
                        StartDate = startDate,
                        EndDate = endDate
                    }
                );
                //Log.Information("ID:{id} == NAME:{name} == DESC:{desc} == ISACTIVE:{isactive} == UPDATEDATE:{updatedate} == STARTDATE:{startdate} == ENDDATE:{enddate}", id, name, desc, isactive, updateDate, startDate, endDate);
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
                    var bcmd = new SqlBatchCommand($"""
                        INSERT INTO {this.objectInfo.TargetTableFullName}(
                            [Id], [Name], [ShortName], [Desc], [IsActive], [UpdateDate], [StartDate], [EndDate]
                        ) VALUES (
                            @Id, @Name, @ShortName, @Desc, @IsActive, @UpdateDate, @StartDate, @EndDate
                        );
                        """
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
