using System.Xml.Linq;
using importGeoEfc.models.garmodels;
using Shared.garfiles;
using static Shared.MyLogger;

namespace importGeoEfc;

static class InitialGarSource  // Singleton !!!
{
    public static GarFiles GarFiles { get; set; }
    public static void InitGarSource()
    {
        string basePath = @"C:\Users\PaNesterov\Documents\dotNetProjects\files\gar\";
        //string basePath = @"/home/pnesterov/my_dev/files/gar/";
        InitialGarSource.GarFiles = new GarFiles(basePath);
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

    public IEnumerable<HouseType> IterHouseTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new HouseType
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
        }
    }
}

class AddrObjTypeEnumerator
{
    public string SourceKey = "AS_ADDR_OBJ_TYPES";
    public string SourceFileFullPath = string.Empty;

    public AddrObjTypeEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<AddrObjType> IterAddrObjTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new AddrObjType
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Level = int.Parse(el.Attribute("LEVEL").Value),
                ShortName = el.Attribute("SHORTNAME").Value,
                Name = el.Attribute("NAME").Value,
                Desc = el.Attribute("DESC").Value,
                UpdateDate = DateOnly.Parse(el.Attribute("UPDATEDATE").Value),
                StartDate = DateOnly.Parse(el.Attribute("STARTDATE").Value),
                EndDate = DateOnly.Parse(el.Attribute("ENDDATE").Value),
                IsActive = bool.Parse(el.Attribute("ISACTIVE").Value),
            };
        }
    }
}

class AppartmentTypeEnumerator
{
    public string SourceKey = "AS_APARTMENT_TYPES";
    public string SourceFileFullPath = string.Empty;

    public AppartmentTypeEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<AppartmentType> IterAppartmentType()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new AppartmentType
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
        }
    }
}

class NormativeDocsKindEnumerator
{
    public string SourceKey = "AS_NORMATIVE_DOCS_KINDS";
    public string SourceFileFullPath = string.Empty;

    public NormativeDocsKindEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<NormativeDocsKind> IterNormativeDocsKinds()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new NormativeDocsKind
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Name = el.Attribute("NAME").Value,
            };
        }
    }
}

class NormativeDocsTypeEnumerator
{
    public string SourceKey = "AS_NORMATIVE_DOCS_TYPES";
    public string SourceFileFullPath = string.Empty;

    public NormativeDocsTypeEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<NormativeDocsType> IterNormativeDocsTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new NormativeDocsType 
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Name = el.Attribute("NAME").Value,
                StartDate = DateOnly.Parse(el.Attribute("STARTDATE").Value),
                EndDate = DateOnly.Parse(el.Attribute("ENDDATE").Value),
            };
        }
    }
}

class ObjectLevelsEnumerator
{
    public string SourceKey = "AS_OBJECT_LEVELS";
    public string SourceFileFullPath = string.Empty;

    public ObjectLevelsEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<ObjectLevel> IterObjectLevels()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new ObjectLevel()
            {
                Level = int.Parse(el.Attribute("LEVEL").Value),
                Name = el.Attribute("NAME").Value,
                UpdateDate = DateOnly.Parse(el.Attribute("UPDATEDATE").Value),
                StartDate = DateOnly.Parse(el.Attribute("STARTDATE").Value),
                EndDate = DateOnly.Parse(el.Attribute("ENDDATE").Value),
                IsActive = bool.Parse(el.Attribute("ISACTIVE").Value),
            };
        }
    }
}

class OperationTypesEnumerator
{
    public string SourceKey = "AS_OPERATION_TYPES";
    public string SourceFileFullPath = string.Empty;

    public OperationTypesEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<OperationType> IterOperationTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new OperationType()
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Name = el.Attribute("NAME").Value,
                UpdateDate = DateOnly.Parse(el.Attribute("UPDATEDATE").Value),
                StartDate = DateOnly.Parse(el.Attribute("STARTDATE").Value),
                EndDate = DateOnly.Parse(el.Attribute("ENDDATE").Value),
                IsActive = bool.Parse(el.Attribute("ISACTIVE").Value),
            };
        }
    }
}

class ParamTypesEnumerator
{
    public string SourceKey = "AS_PARAM_TYPES";
    public string SourceFileFullPath = string.Empty;

    public ParamTypesEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<ParamType> IterParamTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new ParamType()
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Name = el.Attribute("NAME").Value,
                Code = el.Attribute("CODE").Value,
                Desc = el.Attribute("DESC").Value,
                UpdateDate = DateOnly.Parse(el.Attribute("UPDATEDATE").Value),
                StartDate = DateOnly.Parse(el.Attribute("STARTDATE").Value),
                EndDate = DateOnly.Parse(el.Attribute("ENDDATE").Value),
                IsActive = bool.Parse(el.Attribute("ISACTIVE").Value),
            };
        }
    }
}

class RoomTypesEnumerator
{
    public string SourceKey = "AS_ROOM_TYPES";
    public string SourceFileFullPath = string.Empty;

    public RoomTypesEnumerator()
    {
        this.SourceFileFullPath = InitialGarSource.GetMasterFullFilePathByStringKey(this.SourceKey);
    }

    public IEnumerable<RoomType> IterRoomTypes()
    {
        Log.Information("Импорт из файла {filePath}", this.SourceFileFullPath);
        XDocument xDoc = XDocument.Load(this.SourceFileFullPath);
        foreach (XElement el in xDoc.Root.Elements())
        {
            yield return new RoomType()
            {
                Id = int.Parse(el.Attribute("ID").Value),
                Name = el.Attribute("NAME").Value,
                Desc = el.Attribute("DESC").Value,
                IsActive = bool.Parse(el.Attribute("ISACTIVE").Value),
            };
        }
    }
}
