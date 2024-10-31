using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using static Shared.MyLogger;

namespace Shared.connections
{
    public class MsSqlConnection
    {
        public string ConnectionString { get; set; } = string.Empty;
        
        public MsSqlConnection(string connString)
        {
            this.ConnectionString = connString;
        }

        public bool TryMsSqlDbConnection() // 1:1 c target
        {
            Log.Information("Проверка подключения source по строке: {connString}", this.ConnectionString);
            try
            {
                using (SqlConnection conn = new SqlConnection(this.ConnectionString))
                {
                    conn.Open();
                    string connInfo = this.getMsSqlConnectionInformation(conn);
                    Log.Debug("Информация о соединении:\n {conn}", connInfo);
                    using (SqlCommand cmd = new SqlCommand("SELECT GETDATE();", conn))
                    {
                        cmd.CommandType = CommandType.Text;
                        var dt = cmd.ExecuteScalar();
                        Log.Information("Подключено успешно...{dt}", dt);
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error("Ошибка проверки подключения с Базе Данных:\n SQL:{message}", ex.Message);
                return false;
            }
        }

        private string getMsSqlConnectionInformation(SqlConnection cnn)
        {
            StringBuilder sb = new StringBuilder(1024);

            sb.AppendLine("Connection String: " + cnn.ConnectionString);
            sb.AppendLine("State: " + cnn.State.ToString());
            sb.AppendLine("Connection Timeout: " + cnn.ConnectionTimeout.ToString());
            sb.AppendLine("Database: " + cnn.Database);
            sb.AppendLine("Data Source: " + cnn.DataSource);
            sb.AppendLine("Server Version: " + cnn.ServerVersion);

            return sb.ToString();
        }
    }
}
