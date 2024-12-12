using System.ComponentModel.DataAnnotations;

namespace importGeoEfc.models.garmodels;

public class ParamType
{
    [Key]
    public int Id { get; set; }

    [StringLength(50)]
    public string Name { get; set; }

    [StringLength(50)]
    public string Code { get; set; }

    [StringLength(120)]
    public string? Desc { get; set; }

    public DateOnly UpdateDate { get; set; }
    public DateOnly StartDate { get; set; }
    public DateOnly EndDate { get; set; }
    public bool IsActive { get; set; }
}
