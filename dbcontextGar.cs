using Microsoft.EntityFrameworkCore;
using Shared;
using static Shared.MyLogger;
using Shared.connections;
using importGeoEfc.models.garmodels;

namespace importGeoEfc;

public class GarContext : DbContext
{
    public DbSet<HouseType> HouseTypes { get; set; } = null;
    public DbSet<AddrObjType> AddrObjTypes { get; set; } = null;
    public DbSet<AppartmentType> AppartmentTypes { get; set; } = null;
    public DbSet<NormativeDocsKind> NormativeDocsKinds { get; set; } = null;
    public DbSet<NormativeDocsType> NormativeDocsTypes { get; set; } = null;
    public DbSet<ObjectLevel> ObjectLevels { get; set; } = null;
    public DbSet<OperationType> OparationTypes { get; set; } = null;
    public DbSet<ParamType> ParamTypes { get; set; } = null;
    public DbSet<RoomType> RoomTypes { get; set; } = null;

    private TargetDb tdb;

    public GarContext(TargetDb tdb = TargetDb.mssql)
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
        //string connStr = ConnectionStrings.GetConnectionStringByName("kladrStage");
        string connStr = ConnectionStrings.GetConnectionStringByName("garStage");
        Log.Debug("Строка подключения: {connStr}", connStr);

        InitialGarSource.InitGarSource();

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
                    modelBuilder.HasDefaultSchema("stg");
                    modelBuilder.Entity<HouseType>().ToTable("HouseTypes");
                    modelBuilder.Entity<AddrObjType>().ToTable("AddrObjTypes");
                    modelBuilder.Entity<AppartmentType>().ToTable("AppartmentTypes");
                    modelBuilder.Entity<NormativeDocsKind>().ToTable("NormativeDocsKinds");
                    modelBuilder.Entity<NormativeDocsType>().ToTable("NormativeDocsTypes");
                    modelBuilder.Entity<ObjectLevel>().ToTable("ObjectLevels");
                    modelBuilder.Entity<OperationType>().ToTable("OperationTypes");
                    modelBuilder.Entity<ParamType>().ToTable("ParamTypes");
                    modelBuilder.Entity<RoomType>().ToTable("RoomTypes");
                    
                    break;
                }
            case TargetDb.pgsql:
                {
                    modelBuilder.HasDefaultSchema("stg");
                    modelBuilder.Entity<HouseType>().ToTable("house_types");
                    modelBuilder.Entity<HouseType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<HouseType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<HouseType>().Property("ShortName").HasColumnName("shortname");
                    modelBuilder.Entity<HouseType>().Property("Desc").HasColumnName("desc");
                    modelBuilder.Entity<HouseType>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<HouseType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<HouseType>().Property("EndDate").HasColumnName("enddate");
                    modelBuilder.Entity<HouseType>().Property("IsActive").HasColumnName("isactive");

                    modelBuilder.Entity<AddrObjType>().ToTable("addr_obj_types");
                    modelBuilder.Entity<AddrObjType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<AddrObjType>().Property("Level").HasColumnName("level");
                    modelBuilder.Entity<AddrObjType>().Property("ShortName").HasColumnName("shortname");
                    modelBuilder.Entity<AddrObjType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<AddrObjType>().Property("Desc").HasColumnName("desc");
                    modelBuilder.Entity<AddrObjType>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<AddrObjType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<AddrObjType>().Property("EndDate").HasColumnName("enddate");

                    modelBuilder.Entity<AppartmentType>().ToTable("appartment_types");
                    modelBuilder.Entity<AppartmentType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<AppartmentType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<AppartmentType>().Property("ShortName").HasColumnName("shortname");
                    modelBuilder.Entity<AppartmentType>().Property("Desc").HasColumnName("desc");
                    modelBuilder.Entity<AppartmentType>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<AppartmentType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<AppartmentType>().Property("EndDate").HasColumnName("enddate");
                    modelBuilder.Entity<AppartmentType>().Property("IsActive").HasColumnName("isactive");

                    modelBuilder.Entity<NormativeDocsKind>().ToTable("normative_docs_kinds");
                    modelBuilder.Entity<NormativeDocsKind>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<NormativeDocsKind>().Property("Name").HasColumnName("name");

                    modelBuilder.Entity<NormativeDocsType>().ToTable("normative_docs_types");
                    modelBuilder.Entity<NormativeDocsType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<NormativeDocsType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<NormativeDocsType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<NormativeDocsType>().Property("EndDate").HasColumnName("enddate");

                    modelBuilder.Entity<ObjectLevel>().ToTable("object_levels");
                    modelBuilder.Entity<ObjectLevel>().Property("Level").HasColumnName("level");
                    modelBuilder.Entity<ObjectLevel>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<ObjectLevel>().Property("ShortName").HasColumnName("shortname");
                    modelBuilder.Entity<ObjectLevel>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<ObjectLevel>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<ObjectLevel>().Property("EndDate").HasColumnName("enddate");
                    modelBuilder.Entity<ObjectLevel>().Property("IsActive").HasColumnName("isactive");

                    modelBuilder.Entity<OperationType>().ToTable("operation_types");
                    modelBuilder.Entity<OperationType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<OperationType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<OperationType>().Property("ShortName").HasColumnName("shortname");
                    modelBuilder.Entity<OperationType>().Property("Desc").HasColumnName("desc");
                    modelBuilder.Entity<OperationType>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<OperationType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<OperationType>().Property("EndDate").HasColumnName("enddate");
                    modelBuilder.Entity<OperationType>().Property("IsActive").HasColumnName("isactive");

                    modelBuilder.Entity<ParamType>().ToTable("param_types");
                    modelBuilder.Entity<ParamType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<ParamType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<ParamType>().Property("Code").HasColumnName("code");
                    modelBuilder.Entity<ParamType>().Property("Desc").HasColumnName("desc");
                    modelBuilder.Entity<ParamType>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<ParamType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<ParamType>().Property("EndDate").HasColumnName("enddate");
                    modelBuilder.Entity<ParamType>().Property("IsActive").HasColumnName("isactive");

                    modelBuilder.Entity<RoomType>().ToTable("room_types");
                    modelBuilder.Entity<RoomType>().Property("Id").HasColumnName("id");
                    modelBuilder.Entity<RoomType>().Property("Name").HasColumnName("name");
                    modelBuilder.Entity<RoomType>().Property("ShortName").HasColumnName("shortname");
                    modelBuilder.Entity<RoomType>().Property("Desc").HasColumnName("desc");
                    modelBuilder.Entity<RoomType>().Property("UpdateDate").HasColumnName("updatedate");
                    modelBuilder.Entity<RoomType>().Property("StartDate").HasColumnName("startdate");
                    modelBuilder.Entity<RoomType>().Property("EndDate").HasColumnName("enddate");
                    modelBuilder.Entity<RoomType>().Property("IsActive").HasColumnName("isactive");

                    break;
                }
        }
    }
}
