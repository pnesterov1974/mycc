using static Shared.MyLogger;

namespace Shared.garfiles;

public class GarFiles
{
    public Dictionary<string, GarFileInfo?> GarMasters = new Dictionary<string, GarFileInfo?>()
    {
        {"AS_ADDHOUSE_TYPES", null},
        {"AS_ADDR_OBJ_TYPES", null},
        {"AS_APARTMENT_TYPES", null},
        {"AS_HOUSE_TYPES", null},
        {"AS_NORMATIVE_DOCS_KINDS", null},
        {"AS_NORMATIVE_DOCS_TYPES", null},
        {"AS_OBJECT_LEVELS", null},
        {"AS_OPERATION_TYPES", null},
        {"AS_PARAM_TYPES", null},
        {"AS_ROOM_TYPES", null}
    };
    //public List<GarFileInfo> SourceMasterFilePaths { get; set; }
    public string GarPath { get; set; } = string.Empty;

    public GarFiles(string garPath)
    {
        if (Path.Exists(garPath))
        {
            this.GarPath = garPath;
            this.fillGarMastersWithFiles();
        }
        else
        {
            Log.Information("Путь {garPath} не существует", garPath);
            //throw
        }
    }

    private void fillGarMastersWithFiles()
    {
        Log.Information("Поиск справочников...", this.GarPath);
        foreach (var item in this.GarMasters)
        {
            string fileMask = string.Concat(item.Key, "*.XML");
            string[] fileEntries = Directory.GetFiles(this.GarPath, fileMask, SearchOption.AllDirectories);
            Log.Information("Маска файла: {filemask} найдено {cnt} файлов", fileMask, fileEntries.Length);
            if (fileEntries.Length > 0)
            {
                var gfi = new GarFileInfo
                {
                    FileFullPath = fileEntries[0]
                };
                gfi.AnalizeGarFile();
                this.GarMasters[item.Key] = gfi;
            }
        }
    }
/*
    private void FillSourceMasterFilePaths()
    {
        this.SourceMasterFilePaths = new List<GarFileInfo>();
        string[] files = Directory.GetFiles(this.GarPath, "*.xml", SearchOption.TopDirectoryOnly);
        foreach (var f in files)
        {
            var gfi = new GarFileInfo
            {
                FileFullPath = f
            };
            gfi.AnalizeGarFile();
            this.SourceMasterFilePaths.Add(gfi);
        }
    }
*/
    public void PrintMasters()
    {
        foreach(KeyValuePair<string, GarFileInfo?> kvp in this.GarMasters)
        {
            Log.Information("{Key} : {Value}", kvp.Key, kvp.Value);
        }
    }
}
