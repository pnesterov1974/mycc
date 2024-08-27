//using DatameshPgSql;
using DatameshMsSql;
using Shared;
using Shared.Transformations.mssql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseDeveloperExceptionPage();
app.UseSwagger();
app.UseSwaggerUI();

string connString = ConnectionString.GetConnectionString(DBSource.mssql);
MyLogger.InitLogger();

app.MapGet("/", () => "Hello World!");

app.MapGet("/socrbase", () =>
{
    string selectSql = SocrBaseMsSql.SQL;
    MsSqlData msd = new MsSqlData(connString, selectSql);
    return msd.AsJson;
});

app.MapGet("/altnames", () =>
{
    string selectSql = AltNamesMsSql.SQL;
    MsSqlData msd = new MsSqlData(connString, selectSql);
    return msd.AsJson;
});

app.MapGet("/kladr", () =>
{
    string selectSql = KladrMsSql.SQL;
    MsSqlData msd = new MsSqlData(connString, selectSql);
    return msd.AsJson;
});

app.MapGet("/street", () =>
{
    string selectSql = StreetMsSql.SQL;
    MsSqlData msd = new MsSqlData(connString, selectSql);
    return msd.AsJson;
});

app.MapGet("/doma", () =>
{
    string selectSql = DomaMsSql.SQL;
    MsSqlData msd = new MsSqlData(connString, selectSql);
    return msd.AsJson;
});

app.Run();
