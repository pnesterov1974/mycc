using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace importGeoEfc.models.garmodels;

public class RoomType
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public int Id { get; set; }

    [StringLength(100)]
    public string Name { get; set; }

    [StringLength(250)]
    public string? Desc { get; set; }
    public bool IsActive { get; set; }
}
