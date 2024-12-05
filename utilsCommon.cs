using System.Text;
using DbfDataReader;
using static Shared.MyLogger;

namespace Utils;

public class UtilsCommon
{
    public bool ReadDbfInfo(string sourceFilePath)
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
