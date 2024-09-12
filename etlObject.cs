namespace Datamech.mssql;

public class ETLObjectInfo
{
    public string SourceConnectionString { get; set; } = string.Empty;
    public string SourceSelectSql { get; set; } = string.Empty;
    
    public string TargetConnectionString { get; set; } = string.Empty;
    public string TargetSchemaName { get; set; } = string.Empty;
    public string TargetTableName { get; set; } = string.Empty;

    public string TargetTableFullName
    {
    	get
    	{
    		return string.Join('.', this.TargetSchemaName, this.TargetTableName);
    	}
    }
} 
