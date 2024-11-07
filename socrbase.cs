using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace import_kladr_efcms.models;

//[PrimaryKey(nameof(State), nameof(LicensePlate))]
[Table("SOCRBASE", Schema = "dbo")]
public class SocrBase
{
    [Key]
    public int Id { get; set; }

    public int LEVEL { get; set; }

    [StringLength(10)]
    public string SCNAME { get; set; }

    [StringLength(29)]
    public string SOCRNAME { get; set; }

    [StringLength(3)]
    public string KOD_T_ST { get; set; }
}
