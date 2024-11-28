using Microsoft.EntityFrameworkCore;
using Shared;
using Shared.connections;
using import_kladr_ef.models;

namespace import_kladr_ef;

public class KladrContext : DbContext
{
    public DbSet<SocrBase> SocrBases { get; set; } = null;
    public DbSet<AltNames> AltNames { get; set; } = null;
    public DbSet<Kladr> Kladrs { get; set; } = null;
    public DbSet<Street> Streets { get; set; } = null;
    public DbSet<Doma> Domas { get; set; } = null;
    private TargetDb tdb;

    public KladrContext(TargetDb tdb = TargetDb.mssql)
    {
        this.tdb = tdb;
        Database.EnsureCreated();
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!ConnectionStrings.ConnectionStringsInitialized())
        {
            ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql);
        } 
        string connStr = ConnectionStrings.GetConnectionStringByName("kladrStage");

        switch (this.tdb)
        {
            case TargetDb.mssql:
            {
                optionsBuilder.UseSqlServer(connStr);
                break;
            }
            case TargetDb.pgsql:
            {
                optionsBuilder.UseNpgsql(connStr);
                break;
            }
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        switch (this.tdb)
        {
            case TargetDb.mssql:
            {
                modelBuilder.HasDefaultSchema("dbo");
                break;
            }
            case TargetDb.pgsql:
            {
                modelBuilder.HasDefaultSchema("public");
                break;
            }
        }
        modelBuilder.Entity<Kladr>().ToTable("KLADR");
        modelBuilder.Entity<Street>().ToTable("STREET");
        modelBuilder.Entity<Doma>().ToTable("DOMA");
        modelBuilder.Entity<AltNames>().ToTable("ALTNAMES");
        modelBuilder.Entity<SocrBase>().ToTable("SOCRBASE");
    }
}

