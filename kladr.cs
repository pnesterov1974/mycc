using System.ComponentModel.DataAnnotations;
//using System.ComponentModel.DataAnnotations.Schema;

namespace import_kladr_ef.models;

//[PrimaryKey(nameof(State), nameof(LicensePlate))]
//[Table("KLADR", Schema = "dbo")]
public class Kladr
{
    [StringLength(40)]
    public string NAME { get; set; }

    [StringLength(10)]
    public string SOCR { get; set; }

    [Key]
    [StringLength(13)]
    public string CODE { get; set; }

    [StringLength(6)]
    public string INDEX { get; set; }

    [StringLength(4)]
    public string GNINMB { get; set; }

    [StringLength(4)]
    public string UNO { get; set; }

    [StringLength(11)]
    public string OCATD { get; set; }

    public int STATUS { get; set; }
}
