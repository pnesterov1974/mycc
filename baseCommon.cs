using Shared;
using Shared.importModels;
using Utils;

namespace ImportGeo;

public class BaseCommonImport
{
    public IImportModel ImportModel { get; set; }
    public string TargetSchemaName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;
    public string TargetConnectionString { get; set; } = string.Empty;

    public TargetDb TargetDb = TargetDb.mssql;

    public string TargetTableFullName
    {
        get
        {
            return string.Join('.', this.TargetSchemaName, this.TargetTableName);
        }
    }
    
    public IDBUtils DBUtils 
    { 
        get
        {
            switch (this.TargetDb)
            {
                case TargetDb.mssql: return new UtilsMsSql();
                case TargetDb.pgsql: return new UtilsPgSql();
                default: return null;
            }
        }
    }
}
