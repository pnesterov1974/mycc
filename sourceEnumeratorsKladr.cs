using System.Text;
using DbfDataReader;
using importGeoEfc.models.kladr;
using static Shared.MyLogger;
using Shared.importModels;

namespace importGeoEfc;

class BaseSourceEnumerator
{
    protected bool ReadDbfInfo(string sourceFilePath)
    {
        Log.Information("Считывание информации о файле {filepath}", sourceFilePath);
        long recordCount = 0;
        if (File.Exists(sourceFilePath))
        {
            Log.Information("Файл {filepath} найден, обработка...", sourceFilePath);
            using (var dbfTable = new DbfTable(sourceFilePath, Encoding.UTF8))
            {
                var header = dbfTable.Header;
                var versionDescription = header.VersionDescription;
                var hasMemo = dbfTable.Memo != null;
                recordCount = header.RecordCount;
                Log.Information("versionDescription: {versionDescription}\thasMemo: {hasMemo}\trecordCount: {recordCount}", versionDescription, hasMemo, recordCount);
                foreach (var dbfColumn in dbfTable.Columns)
                {
                    var name = dbfColumn.ColumnName;
                    var columnType = dbfColumn.ColumnType;
                    var length = dbfColumn.Length;
                    var decimalCount = dbfColumn.DecimalCount;
                    Log.Information("name: {name}\tcolumnType: {columnType}\tlength: {length}\tdecimalCount: {decimalCount}", name, columnType, length, decimalCount);
                }
            }
            return recordCount > 0;
        }
        else
        {
            Log.Information("Файла {@filepath} не сушествует, останов...", sourceFilePath);
            return false;
        }
    }
}

class KladrSourceEnumerator: BaseSourceEnumerator
{
    public IImportModel ImportModel = new KladrImportModel();

    private DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    public IEnumerable<Kladr> KladrIter()
    {
        string sourceFilePath = this.ImportModel.SourceFullPathName;
        Log.Information("Проверка источника KLADR из {kladrPath}", sourceFilePath);
        if (!this.ReadDbfInfo(sourceFilePath))
        {
            Log.Information("Источник {sourceFile} не валиден. Отмена обработки KLADR", sourceFilePath);
            yield break;
        }
        string DtFileFullPath = this.ImportModel.DtFileFullPath;
        Log.Information("Дата-метка файл {dtFilePath}", DtFileFullPath);
        DateOnly Dt = this.ImportModel.Dt;
        Log.Information("Дата-метка {dt}", Dt);
        Log.Information("Загрузка KLADR из {kladrPath}", sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, this.dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var name = dbfDataReader.GetString(0);
                var socr = dbfDataReader.GetString(1);
                var code = dbfDataReader.GetString(2);
                var index = dbfDataReader.GetString(3);
                var gninmb = dbfDataReader.GetString(4);
                var uno = dbfDataReader.GetString(5);
                var ocatd = dbfDataReader.GetString(6);
                var status = dbfDataReader.GetString(7);

                var kladr = new Kladr
                {
                    NAME = name,
                    SOCR = socr,
                    CODE = code,
                    INDEX = index,
                    GNINMB = gninmb,
                    UNO = uno,
                    OCATD = ocatd,
                    STATUS = int.Parse(status),
                    BusinessDT = Dt
                };
                yield return kladr;
            }
        }
    }
}

class StreetSourceEnumerator: BaseSourceEnumerator
{
    public IImportModel ImportModel = new StreetImportModel();

    private DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    public IEnumerable<Street> StreetIter()
    {
        string sourceFilePath = this.ImportModel.SourceFullPathName;
        Log.Information("Проверка источника KLADR из {kladrPath}", sourceFilePath);
        if (!this.ReadDbfInfo(sourceFilePath))
        {
            Log.Information("Источник {sourceFile} не валиден. Отмена обработки KLADR", sourceFilePath);
            yield break;
        }
        string DtFileFullPath = this.ImportModel.DtFileFullPath;
        Log.Information("Дата-метка файл {dtFilePath}", DtFileFullPath);
        DateOnly Dt = this.ImportModel.Dt;
        Log.Information("Дата-метка {dt}", Dt);
        Log.Information("Загрузка STREET из {kladrPath}", sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, this.dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var name = dbfDataReader.GetString(0);
                var socr = dbfDataReader.GetString(1);
                var code = dbfDataReader.GetString(2);
                var index = dbfDataReader.GetString(3);
                var gninmb = dbfDataReader.GetString(4);
                var uno = dbfDataReader.GetString(5);
                var ocatd = dbfDataReader.GetString(6);

                var street = new Street
                {
                    NAME = name,
                    SOCR = socr,
                    CODE = code,
                    INDEX = index,
                    GNINMB = gninmb,
                    UNO = uno,
                    OCATD = ocatd,
                    BusinessDT = Dt
                };
                yield return street;
            }
        }
    }
}

class DomaSourceEnumerator: BaseSourceEnumerator
{
    public IImportModel ImportModel = new DomaImportModel();

    private DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    public IEnumerable<Doma> DomaIter()
    {
        string sourceFilePath = this.ImportModel.SourceFullPathName;
        Log.Information("Проверка источника KLADR из {kladrPath}", sourceFilePath);
        if (!this.ReadDbfInfo(sourceFilePath))
        {
            Log.Information("Источник {sourceFile} не валиден. Отмена обработки KLADR", sourceFilePath);
            yield break;
        }
        string DtFileFullPath = this.ImportModel.DtFileFullPath;
        Log.Information("Дата-метка файл {dtFilePath}", DtFileFullPath);
        DateOnly Dt = this.ImportModel.Dt;
        Log.Information("Дата-метка {dt}", Dt);
        Log.Information("Загрузка DOMA из {kladrPath}", sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, this.dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var name = dbfDataReader.GetString(0);
                var korp = dbfDataReader.GetString(1);
                var socr = dbfDataReader.GetString(2);
                var code = dbfDataReader.GetString(3);
                var index = dbfDataReader.GetString(4);
                var gninmb = dbfDataReader.GetString(5);
                var uno = dbfDataReader.GetString(6);
                var ocatd = dbfDataReader.GetString(7);

                var doma = new Doma
                {
                    NAME = name,
                    KORP = korp,
                    SOCR = socr,
                    CODE = code,
                    INDEX = index,
                    GNINMB = gninmb,
                    UNO = uno,
                    OCATD = ocatd,
                    BusinessDT = Dt
                };
                yield return doma;
            }
        }
    }
}

class AltNamesSourceEnumerator: BaseSourceEnumerator
{
    public IImportModel ImportModel = new AltNamesImportModel();

    private DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    public IEnumerable<AltNames> AltNamesIter()
    {
        string sourceFilePath = this.ImportModel.SourceFullPathName;
        Log.Information("Проверка источника KLADR из {kladrPath}", sourceFilePath);
        if (!this.ReadDbfInfo(sourceFilePath))
        {
            Log.Information("Источник {sourceFile} не валиден. Отмена обработки KLADR", sourceFilePath);
            yield break;
        }
        string DtFileFullPath = this.ImportModel.DtFileFullPath;
        Log.Information("Дата-метка файл {dtFilePath}", DtFileFullPath);
        DateOnly Dt = this.ImportModel.Dt;
        Log.Information("Дата-метка {dt}", Dt);
        Log.Information("Загрузка AltNames из {kladrPath}", sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, this.dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var oldCode = dbfDataReader.GetString(0);
                var newCode = dbfDataReader.GetString(1);
                var level = dbfDataReader.GetString(2);

                var altNames = new AltNames
                {
                    OLDCODE = oldCode,
                    NEWCODE = newCode,
                    LEVEL = int.Parse(level),
                    BusinessDT = Dt
                };
                yield return altNames;
            }
        }
    }
}

class SocrBaseSourceEnumerator: BaseSourceEnumerator
{
    public IImportModel ImportModel = new SocrBaseImportModel();

    private DbfDataReaderOptions dbfOptions = new DbfDataReaderOptions { SkipDeletedRecords = true };

    public IEnumerable<SocrBase> SocrBaseIter()
    {
        string sourceFilePath = this.ImportModel.SourceFullPathName;
        Log.Information("Проверка источника KLADR из {kladrPath}", sourceFilePath);
        if (!this.ReadDbfInfo(sourceFilePath))
        {
            Log.Information("Источник {sourceFile} не валиден. Отмена обработки SOCRBASE", sourceFilePath);
            yield break;
        }
        string DtFileFullPath = this.ImportModel.DtFileFullPath;
        Log.Information("Дата-метка файл {dtFilePath}", DtFileFullPath);
        DateOnly Dt = this.ImportModel.Dt;
        Log.Information("Дата-метка {dt}", Dt);
        Log.Information("Загрузка SocrBase из {kladrPath}", sourceFilePath);
        using (var dbfDataReader = new DbfDataReader.DbfDataReader(sourceFilePath, this.dbfOptions))
        {
            while (dbfDataReader.Read())
            {
                var level = dbfDataReader.GetString(0);
                var scName = dbfDataReader.GetString(1);
                var socrName = dbfDataReader.GetString(2);
                var kodTst = dbfDataReader.GetString(3);

                var socrBase = new SocrBase
                {
                    LEVEL = int.Parse(level),
                    SCNAME = scName,
                    SOCRNAME = socrName,
                    KOD_T_ST = kodTst,
                    BusinessDT = Dt
                };
                yield return socrBase;
            }
        }
    }
}
