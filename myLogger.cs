using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Shared;

public static class MyLogger
{
    public static Logger Log;

    public static void InitLogger()
    {
        string logFileName = string.Join('_', "log", DateTime.Now.ToString("yyyMMdd_HHmmss_ssms"));
        string logFolder = Path.Join(Directory.GetCurrentDirectory(), "_log");
        if (!Path.Exists(logFolder))
        {
            Directory.CreateDirectory(logFolder);
        }
        string logFilePath = Path.Combine(logFolder, logFileName);
        
        MyLogger.Log = new LoggerConfiguration() //.WriteTo.Console(theme: AnsiConsoleTheme.Code)
            .MinimumLevel.Debug()
            .WriteTo.Console(restrictedToMinimumLevel: LogEventLevel.Information)
            .WriteTo.File(logFilePath)
            .CreateLogger();
    }
}
