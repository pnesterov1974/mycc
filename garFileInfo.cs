using System.Text;

namespace Shared.garfiles;

// Review try-finally-log-exceptions
public class GarFileInfo: IComparable
{
    public string FileFullPath { get; set; } = string.Empty;
    public string FileNameWithoutExtension { get; set; } = string.Empty;
    public DateOnly FileDto { get; set; }
    public string Guid { get; set; }
    public string GarSourceStringKey { get; set; } = string.Empty;

    public void AnalizeGarFile()
    {
        this.FileNameWithoutExtension = Path.GetFileNameWithoutExtension(this.FileFullPath);
        string[] sd = this.FileNameWithoutExtension.Split('_');
        int countParts = sd.Length;
        string guidPart = sd[countParts - 1];
        string datePart = sd[countParts - 2];
        this.FileDto = DateOnly.ParseExact(datePart,  "yyyyMMdd");
        this.Guid = guidPart;
        int workPartLength = countParts - 2;
        string[] wp = new string[workPartLength];
        for (var i = 0; i < workPartLength; i++)
        {
            wp[i] = sd[i];
        }
        this.GarSourceStringKey = string.Join('_', wp);
    }

    public int CompareTo(object? obj)
    {
        if (obj == null) return 1;
        
        GarFileInfo comparedTo = obj as GarFileInfo;
        
        if ((this.FileFullPath != null) && (comparedTo.FileFullPath != null))
            return String.Compare(this.FileFullPath, comparedTo.FileFullPath);
        else
            return -1;
    }

    public override string ToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("===================\n");
        sb.Append($"FullFilePath:\t {this.FileFullPath}\n");
        sb.Append($"FileDto:\t {this.FileDto}\n");
        sb.Append($"Guid:\t {this.Guid}\n");
        sb.Append($"GarSourceStringKey:\t {this.GarSourceStringKey}");
        return sb.ToString();
    }
}
