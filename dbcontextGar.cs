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

    public GarContext(TargetDb tdb = TargetDb.pgsql)
    {
        this.tdb = tdb;
        Database.EnsureCreated();
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!ConnectionStrings.ConnectionStringsInitialized())
        {
            ConnectionStrings.InitConnectionStrings(Shared.TargetDb.pgsql);
        } 
        string connStr = ConnectionStrings.GetConnectionStringByName("kladrStage");
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
                modelBuilder.Entity<HouseType>().Property("Id").HasField("Id");
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
                modelBuilder.Entity<HouseType>().Property("Id").HasField("id");
                modelBuilder.Entity<HouseType>().Property("Name").HasField("name");
                modelBuilder.Entity<HouseType>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<HouseType>().Property("Desc").HasField("desc");
                modelBuilder.Entity<HouseType>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<HouseType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<HouseType>().Property("EndDate").HasField("enddate");
                modelBuilder.Entity<HouseType>().Property("IsActive").HasField("isactive");

                modelBuilder.Entity<AddrObjType>().ToTable("addr_obj_types");
                modelBuilder.Entity<AddrObjType>().Property("Id").HasField("id");
                modelBuilder.Entity<AddrObjType>().Property("Level").HasField("level");
                modelBuilder.Entity<AddrObjType>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<AddrObjType>().Property("Name").HasField("name");
                modelBuilder.Entity<AddrObjType>().Property("Desc").HasField("desc");
                modelBuilder.Entity<AddrObjType>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<AddrObjType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<AddrObjType>().Property("EndDate").HasField("enddate");

                modelBuilder.Entity<AppartmentType>().ToTable("appartment_types");
                modelBuilder.Entity<AppartmentType>().Property("Id").HasField("id");
                modelBuilder.Entity<AppartmentType>().Property("Name").HasField("name");
                modelBuilder.Entity<AppartmentType>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<AppartmentType>().Property("Desc").HasField("desc");
                modelBuilder.Entity<AppartmentType>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<AppartmentType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<AppartmentType>().Property("EndDate").HasField("enddate");
                modelBuilder.Entity<AppartmentType>().Property("IsActive").HasField("isactive");

                modelBuilder.Entity<NormativeDocsKind>().ToTable("normative_docs_kinds");
                modelBuilder.Entity<NormativeDocsKind>().Property("Id").HasField("id");
                modelBuilder.Entity<NormativeDocsKind>().Property("Name").HasField("name");
                
                modelBuilder.Entity<NormativeDocsType>().ToTable("normative_docs_types");
                modelBuilder.Entity<NormativeDocsType>().Property("Id").HasField("id");
                modelBuilder.Entity<NormativeDocsType>().Property("Name").HasField("name");
                modelBuilder.Entity<NormativeDocsType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<NormativeDocsType>().Property("EndDate").HasField("enddate");

                modelBuilder.Entity<ObjectLevel>().ToTable("object_levels");
                modelBuilder.Entity<ObjectLevel>().Property("Level").HasField("level");
                modelBuilder.Entity<ObjectLevel>().Property("Name").HasField("name");
                modelBuilder.Entity<ObjectLevel>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<ObjectLevel>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<ObjectLevel>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<ObjectLevel>().Property("EndDate").HasField("enddate");
                modelBuilder.Entity<ObjectLevel>().Property("IsActive").HasField("isactive");

                modelBuilder.Entity<OperationType>().ToTable("operation_types");
                modelBuilder.Entity<OperationType>().Property("Id").HasField("id");
                modelBuilder.Entity<OperationType>().Property("Name").HasField("name");
                modelBuilder.Entity<OperationType>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<OperationType>().Property("Desc").HasField("desc");
                modelBuilder.Entity<OperationType>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<OperationType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<OperationType>().Property("EndDate").HasField("enddate");
                modelBuilder.Entity<OperationType>().Property("IsActive").HasField("isactive");

                modelBuilder.Entity<ParamType>().ToTable("param_types");
                modelBuilder.Entity<ParamType>().Property("Id").HasField("id");
                modelBuilder.Entity<ParamType>().Property("Name").HasField("name");
                modelBuilder.Entity<ParamType>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<ParamType>().Property("Desc").HasField("desc");
                modelBuilder.Entity<ParamType>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<ParamType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<ParamType>().Property("EndDate").HasField("enddate");
                modelBuilder.Entity<ParamType>().Property("IsActive").HasField("isactive");

                modelBuilder.Entity<RoomType>().ToTable("room_types");
                modelBuilder.Entity<RoomType>().Property("Id").HasField("id");
                modelBuilder.Entity<RoomType>().Property("Name").HasField("name");
                modelBuilder.Entity<RoomType>().Property("ShortName").HasField("shortname");
                modelBuilder.Entity<RoomType>().Property("Desc").HasField("desc");
                modelBuilder.Entity<RoomType>().Property("UpdateDate").HasField("updatedate");
                modelBuilder.Entity<RoomType>().Property("StartDate").HasField("startdate");
                modelBuilder.Entity<RoomType>().Property("EndDate").HasField("enddate");
                modelBuilder.Entity<RoomType>().Property("IsActive").HasField("isactive");
                
                break;
            }
        }
    }
}
