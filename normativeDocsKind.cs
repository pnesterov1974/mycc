using System.ComponentModel.DataAnnotations;

namespace importGeoEfc.models.garmodels;

public class NormativeDocsKind
{
    [Key]
    public int Id { get; set; }

    [StringLength(500)]
    public string Name { get; set; }
}
