public class NDwhError
{
    public string? Msg { get; set; } = string.Empty;
    public string? EventCategoryCode { get; set; } = string.Empty;
    public DateTime? EventDttm { get; set; }
    public string? LogTable { get; set; } = string.Empty;
    public long? LogPk { get; set; }
    public bool? LogSolvedFlag { get; set; }
    public string? Suser { get; set; } = string.Empty;
    public DateTime? DateChange { get; set; }
    public long? SsisExecutionId { get; set; }
}

public static class NDwhErrors
{
    public static string SelectSql = """
            SELECT gdb.[msg],
                gdb.[event_category_code],
                gdb.[event_dttm],
                gdb.[log_table],
                gdb.[log_pk],
                gdb.[log_solved_flag],
                gdb.[suser],
                gdb.[date_change],
                e.[lt] AS [ssis_execution_id]
            FROM [DWH].monitoring.[grafana_dashboard] gdb
            OUTER APPLY (
                SELECT CASE gdb.[log_table]
                            WHEN 'event_log' THEN (
                                SELECT cpi.execution_id
                                FROM [DWH].meta.[event_log] el
                                INNER JOIN [DWH].meta.[control_process_instance] cpi ON cpi.[control_process_id] = el.[control_process_id]
                                WHERE el.[event_log_id] = gdb.[log_pk]
                                )
                            WHEN 'control_process_instance' THEN (
                                SELECT cpi.[execution_id] 
                                FROM [DWH].meta.[control_process_instance] cpi
                                WHERE cpi.[control_process_id] = gdb.[log_pk]
                                ) 
                        END AS [lt]
                ) e
            WHERE CAST(gdb.[event_dttm] AS DATE) >= '20250113' 
            """;

    public static IEnumerable<NDwhError> IterNdwhErrors(string connString)
    {
        using (SqlConnection conn = new SqlConnection(connString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand(NDwhErrors.SelectSql, conn);
            using SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new NDwhError
                    {
                        Msg = reader.GetString(0),
                        EventCategoryCode = reader.GetString(1),
                        EventDttm = reader.GetDateTime(2),
                        LogTable = reader.GetString(3),
                        LogPk = reader.GetInt64(4),
                        LogSolvedFlag = reader.GetBoolean(5),
                        Suser = reader.GetString(6),
                        DateChange = reader.GetDateTime(7),
                        SsisExecutionId = reader.GetInt64(8),
                    };
                    yield return em;
                }
            }
            else
            {
                Console.WriteLine("Данные отсутствуют...");
                yield break;
            }
        }
    }
}
