using Microsoft.EntityFrameworkCore;
using Shared.connections;
using techdbMsSql.models;

namespace techdbMsSql;

public class KladrContext : DbContext
{
    public DbSet<EtlModelExecution> EtlModelExecutions { get; set; } = null;

    public KladrContext()
    {
        Database.EnsureCreated();
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        //base.OnConfiguring(optionsBuilder);
        if (!ConnectionStrings.ConnectionStringsInitialized())
        {
            ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql);
        } 
        string connStr = ConnectionStrings.GetConnectionStringByName("techDb");
        optionsBuilder.UseSqlServer(connStr);
    }
}
