using Microsoft.EntityFrameworkCore;
using Shared.connections;
using import_kladr_efc.models;

public class KladrContext : DbContext
{
    public DbSet<Kladr> Kladrs { get; set; } = null;//Set<SocrBase>();
    public DbSet<Street> Streets { get; set; } = null;//Set<SocrBase>();
    public DbSet<Doma> Domas { get; set; } = null;//Set<SocrBase>();

    public KladrContext()
    {
        Database.EnsureCreated();
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        //base.OnConfiguring(optionsBuilder);
        ConnectionStrings.InitConnectionStrings(Shared.TargetDb.mssql);
        string connStr = ConnectionStrings.GetConnectionStringByName("kladrRaw");
        optionsBuilder.UseSqlServer(connStr);
    }
}
