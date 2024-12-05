namespace Utils;

public interface IDBUtils
{
    public bool ClearDestTable(string connectionString, string targetTableFullName);

    public bool TryDbConnection(string connectionString);
}
