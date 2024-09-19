using Import.gar.mssql;
using Serilog;
using Shared;

namespace ImportGar.mssql;

public class ImportGarToMsSql : GarSource
{
    public ImportGarToMsSql(string sourceDirPath) : base(sourceDirPath) {; }

    public void DoImport(string connectionString)
    {
        if (this.Masters["AS_NORMATIVE_DOCS_KINDS"] != null)
        {
            ImportObjectInfo NormativeDocsKinds = new ImportObjectInfo
            {
                TargetTableName = "[NormativeDocsKinds]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_NORMATIVE_DOCS_KINDS"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportNormativeDocsKinds ndk = new ImportNormativeDocsKinds(NormativeDocsKinds);
            ndk.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_NORMATIVE_DOCS_KINDS ничего не найдено");
        }

        if (this.Masters["AS_ADDHOUSE_TYPES"] != null)
        {
            ImportObjectInfo AddHouseTypes = new ImportObjectInfo
            {
                TargetTableName = "[AddHouseTypes]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_ADDHOUSE_TYPES"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportAddHouseTypes aht = new ImportAddHouseTypes(AddHouseTypes);
            aht.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_ADDHOUSE_TYPES ничего не найдено");
        }

        if (this.Masters["AS_ADDR_OBJ_TYPES"] != null)
        {
            ImportObjectInfo AddrObjTypes = new ImportObjectInfo
            {
                TargetTableName = "[AddrObjTypes]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_ADDR_OBJ_TYPES"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportAddrObjTypes aot = new ImportAddrObjTypes(AddrObjTypes);
            aot.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_ADDR_OBJ_TYPES ничего не найдено");
        }

        if (this.Masters["AS_APARTMENT_TYPES"] != null)
        {
            ImportObjectInfo AppartmentTypes = new ImportObjectInfo
            {
                TargetTableName = "[AppartmentTypes]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_APARTMENT_TYPES"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportAppartmentTypes apt = new ImportAppartmentTypes(AppartmentTypes);
            apt.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_APARTMENT_TYPES ничего не найдено");
        }

        /*
                        ObjectInfo HouseTypes = new ObjectInfo
                        {
                            DestinationTableName = "house_types",
                            DestinationSchemaName = "gar",
                            SourceFileName = this.Masters["AS_HOUSE_TYPES"].FileName,
                            SourceDirPath = this.SourceDirPath,
                            ConnectionString = connectionString
                        };
                        ImportHouseTypes ht = new ImportHouseTypes(HouseTypes);
                        ht.DoImport();
*/
        if (this.Masters["AS_NORMATIVE_DOCS_TYPES"] != null)
        {
            ImportObjectInfo NormativeDocsTypes = new ImportObjectInfo
            {
                TargetTableName = "NormativeDocsTypes",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_NORMATIVE_DOCS_TYPES"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportNormativeDocsTypes ndt = new ImportNormativeDocsTypes(NormativeDocsTypes);
            ndt.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_NORMATIVE_DOCS_TYPES ничего не найдено");
        }

        if (this.Masters["AS_OBJECT_LEVELS"] != null)
        {
            ImportObjectInfo ObjectLevels = new ImportObjectInfo
            {
                TargetTableName = "[ObjectLevels]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_OBJECT_LEVELS"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportObjectLevels ol = new ImportObjectLevels(ObjectLevels);
            ol.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_OBJECT_LEVELS ничего не найдено");
        }

        if (this.Masters["AS_OPERATION_TYPES"] != null)
        {
            ImportObjectInfo OparationTypes = new ImportObjectInfo
            {
                TargetTableName = "[OperationTypes]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_OPERATION_TYPES"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportOperationTypes opt = new ImportOperationTypes(OparationTypes);
            opt.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_OPERATION_TYPES ничего не найдено");
        }

        /*
                                        ObjectInfo ParamTypes = new ObjectInfo
                                        {
                                            DestinationTableName = "param_types",
                                            DestinationSchemaName = "gar",
                                            SourceFileName = this.Masters["AS_PARAM_TYPES"].FileName,
                                            SourceDirPath = this.SourceDirPath,
                                            ConnectionString = connectionString
                                        };
                                        ImportParamTypes prt = new ImportParamTypes(ParamTypes);
                                        prt.DoImport();
                */
        if (this.Masters["AS_ROOM_TYPES"] != null)
        {
            ImportObjectInfo RoomTypes = new ImportObjectInfo
            {
                TargetTableName = "[RoomTypes]",
                TargetSchemaName = "gar",
                SourceFileName = this.Masters["AS_ROOM_TYPES"].FileName,
                SourceDirPath = this.SourceDirPath,
                ConnectionString = connectionString
            };
            ImportRoomTypes rmt = new ImportRoomTypes(RoomTypes);
            rmt.DoImport();
        }
        else
        {
            Log.Information("По ключу AS_ROOM_TYPES ничего не найдено");
        }
    }
}
