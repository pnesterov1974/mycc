using Shared;
using Serilog;
using System.Xml;

namespace Data.xmlToMsSql;

public class ImportOneFromXML
{
    public ImportOneFromXML(ObjectInfo oi) => this.objectInfo = oi;

    private ObjectInfo objectInfo { get; set; }

    public ILogger Log { get; set; }

    public void DoImportNormativeDocsKinds()
    {
        Log.Information("Импорт из файла {file}", this.objectInfo.SourceFilePath);
        if (File.Exists(this.objectInfo.SourceFilePath))
        {
            Log.Information("Файл существует...Импорт...");

            XmlDocument xDoc = new XmlDocument();
            xDoc.Load(this.objectInfo.SourceFilePath);
            XmlElement? xRoot = xDoc.DocumentElement;
            if (xRoot != null)
            {
                foreach(XmlElement xnode in xRoot)
                {
                    XmlNode? idAttr = xnode.Attributes.GetNamedItem("ID");
                    //string? id = idAttr?.Value;
                    int? id = int.Parse(idAttr?.Value);
                    XmlNode? NameAttr = xnode.Attributes.GetNamedItem("NAME");
                    string? name = NameAttr?.Value;
                    Log.Information("ID: {id} ==== NAME: {name}", id, name);
                }
            }
        }
    }

    public void DoImportAddHouseTypes()
    {
        Log.Information("Импорт из файла {file}", this.objectInfo.SourceFilePath);
        if (File.Exists(this.objectInfo.SourceFilePath))
        {
            Log.Information("Файл существует...Импорт...");

            XmlDocument xDoc = new XmlDocument();
            xDoc.Load(this.objectInfo.SourceFilePath);
            XmlElement? xRoot = xDoc.DocumentElement;
            if (xRoot != null)
            {
                foreach(XmlElement xnode in xRoot)
                {
                    XmlNode? idAttr = xnode.Attributes.GetNamedItem("ID");
                    int? id = int.Parse(idAttr?.Value);
                    XmlNode? NameAttr = xnode.Attributes.GetNamedItem("NAME");
                    string? name = NameAttr?.Value;
                    XmlNode? ShortNameAttr = xnode.Attributes.GetNamedItem("SHORTNAME");
                    string? shortname = ShortNameAttr?.Value;
                    XmlNode? DescAttr = xnode.Attributes.GetNamedItem("DESC");
                    string? desc = DescAttr?.Value;
                    XmlNode? IsActiveAttr = xnode.Attributes.GetNamedItem("ISACTIVE");
                    string? isActive = IsActiveAttr?.Value;

                    XmlNode? UpdateDateAttr = xnode.Attributes.GetNamedItem("UPDATEDATE");
                    string? updateDate = UpdateDateAttr?.Value;
                    XmlNode? StartDateAttr = xnode.Attributes.GetNamedItem("STARTDATE");
                    string? startDate = StartDateAttr?.Value;
                    XmlNode? EndDateAttr = xnode.Attributes.GetNamedItem("ENDDATE");
                    string? endDate = EndDateAttr?.Value;
                    Log.Information("ID:{id} == NAME:{name} == SHORTNAME:{shortname} == DESC:{desc} == ISACTIVE:{isactive} == UPDATEDATE:{updatedate} == STARTDATE:{startdate} == ENDDATE:{enddate}", id, name, shortname, desc, isActive, updateDate, startDate, endDate);
                }
            }
        }
    }

    public void DoImportTypes1()
    {
        Log.Information("Импорт из файла {file}", this.objectInfo.SourceFilePath);
        if (File.Exists(this.objectInfo.SourceFilePath))
        {
            Log.Information("Файл существует...Импорт...");

            XmlDocument xDoc = new XmlDocument();
            xDoc.Load(this.objectInfo.SourceFilePath);
            XmlElement? xRoot = xDoc.DocumentElement;
            if (xRoot != null)
            {
                foreach(XmlElement xnode in xRoot)
                {
                    XmlNode? idAttr = xnode.Attributes.GetNamedItem("ID");
                    int? id = int.Parse(idAttr?.Value);
                    XmlNode? NameAttr = xnode.Attributes.GetNamedItem("NAME");
                    string? name = NameAttr?.Value;
                    XmlNode? ShortNameAttr = xnode.Attributes.GetNamedItem("SHORTNAME");
                    string? shortname = ShortNameAttr?.Value;
                    XmlNode? DescAttr = xnode.Attributes.GetNamedItem("DESC");
                    string? desc = DescAttr?.Value;
                    XmlNode? IsActiveAttr = xnode.Attributes.GetNamedItem("ISACTIVE");
                    string? isActive = IsActiveAttr?.Value;
                    XmlNode? StartDateAttr = xnode.Attributes.GetNamedItem("STARTDATE");
                    string? startDate = StartDateAttr?.Value;
                    XmlNode? EndDateAttr = xnode.Attributes.GetNamedItem("ENDDATE");
                    string? endDate = EndDateAttr?.Value;
                    XmlNode? UpdateDateAttr = xnode.Attributes.GetNamedItem("UPDATEDATE");
                    string? updateDate = UpdateDateAttr?.Value;
                    Log.Information("ID:{id} == NAME:{name} == SHORTNAME:{shortname} == DESC:{desc} == ISACTIVE:{isactive} == UPDATEDATE:{updatedate} == STARTDATE:{startdate} == ENDDATE:{enddate}", id, name, shortname, desc, isActive, updateDate, startDate, endDate);
                }
            }
        }
    }

    public void DoImportTypes2()
    {
        Log.Information("Импорт из файла {file}", this.objectInfo.SourceFilePath);
        if (File.Exists(this.objectInfo.SourceFilePath))
        {
            Log.Information("Файл существует...Импорт...");

            XmlDocument xDoc = new XmlDocument();
            xDoc.Load(this.objectInfo.SourceFilePath);
            XmlElement? xRoot = xDoc.DocumentElement;
            if (xRoot != null)
            {
                foreach(XmlElement xnode in xRoot)
                {
                    XmlNode? idAttr = xnode.Attributes.GetNamedItem("ID");
                    int? id = int.Parse(idAttr?.Value);
                    XmlNode? NameAttr = xnode.Attributes.GetNamedItem("NAME");
                    string? name = NameAttr?.Value;
                    XmlNode? StartDateAttr = xnode.Attributes.GetNamedItem("STARTDATE");
                    string? startDate = StartDateAttr?.Value;
                    XmlNode? EndDateAttr = xnode.Attributes.GetNamedItem("ENDDATE");
                    string? endDate = EndDateAttr?.Value;
                    Log.Information("ID:{id} == NAME:{name} == STARTDATE:{startdate} == ENDDATE:{enddate}", id, name, startDate, endDate);
                }
            }
        }
    }
}
