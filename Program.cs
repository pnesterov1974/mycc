using Datamech;
using Datamech.mssql;
using Etl;
using static Shared.MyLogger;

InitLogger();

ETLObjectInfo etlo = new ETLObjectInfo
{
    SourceConnectionString = ConnectionStrings.etlMsSqlSourceConnectionString,
    SourceSelectSql = StringAssets.clientfaceSQL,
    TargetConnectionString = ConnectionStrings.etlMsSqlTargetConnectionString,
    TargetSchemaName = "dbo",
    TargetTableName = "[test]"

};

var etlw = new EtlWork
{
    Etlo = etlo
};
etlw.Work();
