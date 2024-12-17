using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace importGeoEfc.models.garmodels;

public class ObjectLevel
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public int Level { get; set; }

    [StringLength(250)]
    public string Name { get; set; }

    public DateOnly UpdateDate { get; set; }
    public DateOnly StartDate { get; set; }
    public DateOnly EndDate { get; set; }
    public bool IsActive { get; set; }
}
