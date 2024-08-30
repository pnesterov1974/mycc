using DatameshMsSql;
using DatameshPgSql;
using Shared;
using Shared.Transformations.mssql;
using static Shared.MyLogger;
using Shared.Transformations.pqsql;

const TargetDb tdb = TargetDb.mssql;
string connString = ConnectionString.GetConnectionString(tdb);
MyLogger.InitLogger();

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseDeveloperExceptionPage();
app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/", () => "Hello World!");

app.MapGet("/socrbase", () =>
{
    switch (tdb)
    {
        case TargetDb.mssql:
            MsSqlData msd = new MsSqlData(connString, SocrBaseMsSql.SQL);
            return msd.AsJson;
        case TargetDb.pgsql:
            return "";
            // selectSql = SocrBasePgSql.SQL;
            // PgSqlData psd = new PgSqlData(connString, selectSql);
            // return psd.AsJson;
    }
});

app.MapGet("/altnames", () =>
{
    switch (tdb)
    {
        case TargetDb.mssql:
            Log.Debug("Altnames SQL: \n", AltNamesMsSql.SQL);
            MsSqlData msd = new MsSqlData(connString, AltNamesMsSql.SQL);
            return msd.AsJson;
        case TargetDb.pgsql:
            return "";
            // selectSql = AltNamesPgSql.SQL;
            // PgSqlData psd = new PgSqlData(connString, selectSql);
            // return psd.AsJson;
    }
});

app.MapGet("/kladr", () =>
{
    switch (tdb)
    {
        case TargetDb.mssql:
            MsSqlData msd = new MsSqlData(connString, KladrMsSql.SQL);
            return msd.AsJson;
        case TargetDb.pgsql:
            return "";
            // selectSql = KladrPgSql.SQL;
            // PgSqlData psd = new PgSqlData(connString, selectSql);
            // return psd.AsJson;
    }
});

app.MapGet("/street", () =>
{
    switch (tdb)
    {
        case TargetDb.mssql:
            MsSqlData msd = new MsSqlData(connString, StreetMsSql.SQL);
            return msd.AsJson;
        case TargetDb.pgsql:
            return "";
            // selectSql = StreetPgSql.SQL;
            // PgSqlData psd = new PgSqlData(connString, selectSql);
            // return psd.AsJson;
    }
});

app.MapGet("/doma", () =>
{
    switch (tdb)
    {
        case TargetDb.mssql:
            MsSqlData msd = new MsSqlData(connString, DomaMsSql.SQL);
            return msd.AsJson;
        case TargetDb.pgsql:
            return "";
            // selectSql = DomaPgSql.SQL;
            // PgSqlData psd = new PgSqlData(connString, selectSql);
            // return psd.AsJson;
    }
});

app.Run();
