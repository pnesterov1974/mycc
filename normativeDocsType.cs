using System.ComponentModel.DataAnnotations;

namespace importGeoEfc.models.garmodels;

public class NormativeDocsType
{
    [Key]
    public int Id { get; set; }

    [StringLength(500)]
    public string Name { get; set; }

    public DateOnly StartDate { get; set; }

    public DateOnly EndDate { get; set; }
}
