using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace techdbMsSql.models;

[Table("EtlModelsExecutions", Schema = "dbo")]
public class EtlModelExecution
{
    [Key]
    public string PID { get; set; }

    [StringLength(250)]
    public string ModelName { get; set; }

    public DateTime ExecStart { get; set; }

    public DateTime ExecEnd { get; set; }

    public int ExecStatus { get; set; }

    public int RowsAffwcted { get; set; }
}
