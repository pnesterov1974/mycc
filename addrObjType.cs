using System.ComponentModel.DataAnnotations;

namespace importGeoEfc.models.garmodels;

public class AddrObjType
{
    [Key]
    public int Id { get; set; }

    public int Level { get; set; }

    [StringLength(50)]
    public string ShortName { get; set; }

    [StringLength(250)]
    public string Name { get; set; }

    [StringLength(250)]
    public string? Desc { get; set; }

    public DateOnly UpdateDate { get; set; }
    public DateOnly StartDate { get; set; }
    public DateOnly EndDate { get; set; }
    public bool IsActive { get; set; }
}