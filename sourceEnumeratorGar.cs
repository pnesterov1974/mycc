using System.Xml.Linq;
using importGeoEfc.models.garmodels;
using Shared.garfiles;
using static Shared.MyLogger;

namespace importGeoEfc;

static class InitialGarSource
{
    public static GarFiles GarFiles { get; set; }
    public static void InitGarSource()
    {
        string basePath = @"C:\Users\PaNesterov\Documents\dotNetProjects\files\gar\";
        GarFiles = new GarFiles(basePath);
    }

    public static string GetMasterFullFilePathByStringKey(string sourceStringKey)
    {
        return GarFiles.GarMasters[sourceStringKey].FileFullPath;
    }
}

class HouseTypesEnumerator
{
    public string SourceKey = "AS_HOUSE_TYPES";
    public string SourceFileFullPath = string.Empty;

    public HouseTypesEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<HouseType> HouseTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            HouseType ht = new HouseType
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Name = el.Attribute("NAME").Value,
                ShortName = el.Attribute("SHORTNAME").Value,
                Desc = el.Attribute("DESC").Value,
                UpdateDate = DateOnly.Parse(el.Attribute("UPDATEDATE").Value),
                StartDate = DateOnly.Parse(el.Attribute("STARTDATE").Value),
                EndDate = DateOnly.Parse(el.Attribute("ENDDATE").Value),
                IsActive = bool.Parse(el.Attribute("ISACTIVE").Value),
            };
            yield return ht;
        }
    }
}
