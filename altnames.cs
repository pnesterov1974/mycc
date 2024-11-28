using System.ComponentModel.DataAnnotations;

namespace import_kladr_ef.models;

//[PrimaryKey(nameof(State), nameof(LicensePlate))]
//[Table("ALTNAMES", Schema = "dbo")]
public class AltNames
{
    [Key]
    public int Id { get; set; }

    [StringLength(19)]
    public string OLDCODE { get; set; }

    [StringLength(19)]
    public string NEWCODE { get; set; }

    public int LEVEL { get; set; }
}
