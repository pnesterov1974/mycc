using System.Xml;
using System.Data;
using Data;
using Microsoft.Data.SqlClient;
using Shared;
using static Shared.MyLogger;
using xmlUtils2;
using Data.Import.gar;

namespace Import.gar.mssql;
public class ImportRoomTypes : BaseMsSql
{
    protected ImportObjectInfo objectInfo { get; set; }
    public ImportRoomTypes(ImportObjectInfo objectInfo) => this.objectInfo = objectInfo;

    private List<RoomTypes> GetSourceData(out int ErrorsCount)
    {
        ErrorsCount = 0;
        List<RoomTypes> res = new List<RoomTypes>();
        Log.Information("Импорт из файла ...{file}", this.objectInfo.SourceFileName);
        XmlDocument xDoc = new XmlDocument();
        xDoc.Load(this.objectInfo.SourceFilePath);
        XmlElement? xRoot = xDoc.DocumentElement;
        if (xRoot != null)
        {
            foreach (XmlElement xnode in xRoot)
            {
                int id = XMLUtils.GetIntNotNullNodeValue(xnode, "ID", this.objectInfo.SourceFileName, "ключевое поле", ref ErrorsCount);
                string name = XMLUtils.GetStringNotNullNodeValue(xnode, "NAME", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                string? shortname = XMLUtils.GetStringNullNodeValue(xnode, "SHORTNAME", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                string? desc = XMLUtils.GetStringNullNodeValue(xnode, "DESC", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                bool isactive = (bool)XMLUtils.GetBoolNullNodeValue(xnode, "ISACTIVE", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                DateOnly updateDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "UPDATEDATE", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                DateOnly startDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "STARTDATE", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);
                DateOnly endDate = XMLUtils.GetDateOnlyNotNullNodeValue(xnode, "ENDDATE", this.objectInfo.SourceFileName, id.ToString(), ref ErrorsCount);

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
                        INSERT INTO {this.objectInfo.TargetTableFullName}(
                            [Id], [Name], [ShortName], [Desc], [IsActive], [UpdateDate], [StartDate], [EndDate]
                        ) VALUES (
                            @Id, @Name, @ShortName, @Desc, @IsActive, @UpdateDate, @StartDate, @EndDate
                        );
                        """
                        );

                    SqlParameter pId = new SqlParameter
                    {
                        ParameterName = "@Id",
                        Direction = ParameterDirection.Input,
                        SqlValue = d.Id,
                        SqlDbType = SqlDbType.Int
                    };

                    SqlParameter pName = new SqlParameter
                    {
                        ParameterName = "@Name",
                        Direction = ParameterDirection.Input,
                        SqlValue = d.Name,
                        SqlDbType = SqlDbType.NVarChar
                    };

                    SqlParameter pShortName = new SqlParameter
                    {
                        ParameterName = "@ShortName",
                        Direction = ParameterDirection.Input,
                        IsNullable = true,
                        SqlValue = d.ShortName==null ? DBNull.Value : d.ShortName,
                        SqlDbType = SqlDbType.NVarChar
                    };

                    SqlParameter pDesc = new SqlParameter
                    {
                        ParameterName = "@Desc",
                        Direction = ParameterDirection.Input,
                        IsNullable = true,
                        SqlValue = d.Desc==null ? DBNull.Value : d.Desc,
                        SqlDbType = SqlDbType.NVarChar
                    };

                    SqlParameter pIsActive = new SqlParameter
                    {
                        ParameterName = "@IsActive",
                        Direction = ParameterDirection.Input,
                        SqlValue = d.IsActive,
                        SqlDbType = SqlDbType.Bit
                    };

                    SqlParameter pUpdateDate = new SqlParameter
                    {
                        ParameterName = "@UpdateDate",
                        Direction = ParameterDirection.Input,
                        SqlValue = d.UpdateDate,
                        SqlDbType = SqlDbType.Date
                    };

                    SqlParameter pStartDate = new SqlParameter
                    {
                        ParameterName = "@StartDate",
                        Direction = ParameterDirection.Input,
                        SqlValue = d.StartDate,
                        SqlDbType = SqlDbType.Date
                    };

                    SqlParameter pEndDate = new SqlParameter
                    {
                        ParameterName = "@EndDate",
                        Direction = ParameterDirection.Input,
                        SqlValue = d.EndDate,
                        SqlDbType = SqlDbType.Date
                    };

                    bcmd.Parameters.Add(pId);
                    bcmd.Parameters.Add(pName);
                    bcmd.Parameters.Add(pShortName);
                    bcmd.Parameters.Add(pDesc);
                    bcmd.Parameters.Add(pIsActive);
                    bcmd.Parameters.Add(pStartDate);
                    bcmd.Parameters.Add(pUpdateDate);
                    bcmd.Parameters.Add(pEndDate);

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
