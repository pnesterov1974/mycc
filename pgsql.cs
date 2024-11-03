using System.Data;
using System.Text;
using Npgsql;
using static Shared.MyLogger;

namespace Shared.connections
{
    public class PgConnection
    {
        public string ConnectionString { get; set; } = string.Empty;

        public PgConnection(string connString)
        {
            this.ConnectionString = connString;
        }
        
        public bool TryPgSqlDbConnection()
        {
            Log.Information("Проверка подключения source по строке: {connString}", this.ConnectionString);
            try
            {
                using (NpgsqlConnection conn = new NpgsqlConnection(this.ConnectionString))
                {
                    conn.Open();
                    string connInfo = this.getPgSqlConnectionInformation(conn);
                    Log.Debug("Информация о соединении:\n {conn}", connInfo);
                    using (NpgsqlCommand cmd = new NpgsqlCommand("SELECT NOW();", conn))
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

        private string getPgSqlConnectionInformation(NpgsqlConnection cnn) // 1:1 c target
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

        public bool CheckPgSelectSql(string selectSql)
        {
            Log.Information("Тестирование корректности базового SelectSQL");
            Log.Debug(selectSql);

            using (NpgsqlConnection conn = new NpgsqlConnection(this.ConnectionString))
            {
                using (NpgsqlCommand cmd = new NpgsqlCommand(selectSql, conn))
                {
                    try
                    {
                        conn.Open();
                        DataTable dt;
                        using (NpgsqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                        {
                            dt = rdr.GetSchemaTable();
                            Log.Information("SelectSql корректен");
                            return true;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Debug("Ошибка чтения даннтых \n {errmes}", ex.Message);
                        return false;
                    }
                }
            }
        }
    }
}
