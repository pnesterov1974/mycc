public class SsisErrorMessage
{
    public long OperationId;
    public DateTimeOffset? MessageTime;
    public int MessageType;
    public string? Message;
    public string? PackageName;
    public string? EventName;
    public string? MessageSourceName;
    public string? SubComponentName;
    public string? PackagePath;
    public string? ExecutionPath;
    public int? MessageCode;
}

public static class SsisErrorMessages
{
    public static string SelectSql { get; set; } = """
            SELECT [operation_id], 
                   [message_time], 
                   [message_type], 
                   [message], 
                   [package_name], 
                   [event_name], 
                   [message_source_name], 
                   [subcomponent_name], 
                   [package_path], 
                   [execution_path], 
                   [message_code]
            FROM SSISDB.catalog.[event_messages] em 
            WHERE (em.[operation_id] IS NOT NULL)
                  AND (em.[event_name] = 'OnError')
                  AND (em.[message_code] = 0)
            ORDER BY em.[event_message_id] DESC
            OPTION (RECOMPILE);
            """;


    public static IEnumerable<SsisErrorMessage> IterSsisErrorMessages(string connString)
    {
        using (SqlConnection conn = new SqlConnection(connString))
        {
            conn.Open();
            SqlCommand command = new SqlCommand(SsisErrorMessages.SelectSql, conn);
            using SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    var em = new SsisErrorMessage
                    {
                        OperationId = reader.GetFieldValue<long>(0),
                        MessageTime = reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1),
                        MessageType = reader.GetInt16(2),
                        Message = reader.IsDBNull(3) ? null : reader.GetFieldValue<string>(3),
                        PackageName = reader.IsDBNull(4) ? null :reader.GetFieldValue<string>(4),
                        EventName = reader.IsDBNull(5) ? null :reader.GetFieldValue<string>(5),
                        MessageSourceName = reader.IsDBNull(6) ? null : reader.GetFieldValue<string>(6),
                        SubComponentName = reader.IsDBNull(7) ? null : reader.GetFieldValue<string>(7),
                        PackagePath = reader.IsDBNull(8) ? null :reader.GetFieldValue<string>(8),
                        ExecutionPath = reader.IsDBNull(9) ? null : reader.GetFieldValue<string>(9),
                        MessageCode = reader.IsDBNull(10) ? null : reader.GetFieldValue<int>(10),
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
