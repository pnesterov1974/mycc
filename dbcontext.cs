using Microsoft.EntityFrameworkCore;
using Shared.connections;
using import_kladr_efcms.models;

namespace import_kladr_efcms;

public class KladrContext : DbContext
{
    public DbSet<SocrBase> SocrBases { get; set; } = null;
    public DbSet<AltNames> AltNames { get; set; } = null;
    public DbSet<Kladr> Kladrs { get; set; } = null;
    public DbSet<Street> Streets { get; set; } = null;
    public DbSet<Doma> Domas { get; set; } = null;

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
        string connStr = ConnectionStrings.GetConnectionStringByName("kladrRaw");
        optionsBuilder.UseSqlServer(connStr);
    }
}

