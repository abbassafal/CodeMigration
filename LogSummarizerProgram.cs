using System;
using System.IO;
using DataMigration.Services;

namespace DataMigration
{
    /// <summary>
    /// Console utility to summarize migration log files
    /// Usage: 
    ///   LogSummarizer.exe <logfile> [output.txt] [output.csv]
    /// Example:
    ///   LogSummarizer.exe migration_logs.txt summary.txt summary.csv
    /// </summary>
    class LogSummarizerProgram
    {
        static void Main(string[] args)
        {
            Console.WriteLine("═══════════════════════════════════════════════════════════════════");
            Console.WriteLine("              MIGRATION LOG SUMMARIZER UTILITY                     ");
            Console.WriteLine("═══════════════════════════════════════════════════════════════════\n");

            if (args.Length == 0)
            {
                ShowUsage();
                return;
            }

            string logFilePath = args[0];
            string? txtOutputPath = args.Length > 1 ? args[1] : null;
            string? csvOutputPath = args.Length > 2 ? args[2] : null;

            // Default output paths if not provided
            if (txtOutputPath == null)
            {
                txtOutputPath = Path.Combine(
                    Path.GetDirectoryName(logFilePath) ?? ".",
                    $"{Path.GetFileNameWithoutExtension(logFilePath)}_summary.txt"
                );
            }

            if (csvOutputPath == null)
            {
                csvOutputPath = Path.Combine(
                    Path.GetDirectoryName(logFilePath) ?? ".",
                    $"{Path.GetFileNameWithoutExtension(logFilePath)}_summary.csv"
                );
            }

            try
            {
                var summarizer = new LogSummarizerService();

                // Generate text summary
                Console.WriteLine("\n🔄 Generating text summary...");
                var summary = summarizer.SummarizeLogFile(logFilePath, txtOutputPath);
                
                // Display summary to console
                Console.WriteLine("\n" + summary);

                // Generate CSV summary
                Console.WriteLine("\n🔄 Generating CSV summary...");
                summarizer.GenerateCsvSummary(logFilePath, csvOutputPath);

                Console.WriteLine("\n═══════════════════════════════════════════════════════════════════");
                Console.WriteLine("✅ SUMMARY GENERATION COMPLETED SUCCESSFULLY!");
                Console.WriteLine("═══════════════════════════════════════════════════════════════════");
                Console.WriteLine($"\n📄 Text Summary: {txtOutputPath}");
                Console.WriteLine($"📊 CSV Summary: {csvOutputPath}");
                Console.WriteLine($"\n💡 Tip: Open the CSV file in Excel for easier analysis.\n");
            }
            catch (FileNotFoundException ex)
            {
                Console.WriteLine($"\n❌ ERROR: {ex.Message}");
                Environment.Exit(1);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ ERROR: An unexpected error occurred:");
                Console.WriteLine($"   {ex.Message}");
                Console.WriteLine($"\n   Stack Trace:\n{ex.StackTrace}");
                Environment.Exit(1);
            }
        }

        static void ShowUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run <logfile> [output.txt] [output.csv]");
            Console.WriteLine();
            Console.WriteLine("Arguments:");
            Console.WriteLine("  <logfile>      Path to the migration log file (required)");
            Console.WriteLine("  [output.txt]   Path for text summary output (optional)");
            Console.WriteLine("  [output.csv]   Path for CSV summary output (optional)");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  dotnet run migration_logs.txt");
            Console.WriteLine("  dotnet run migration_logs.txt summary.txt");
            Console.WriteLine("  dotnet run migration_logs.txt summary.txt summary.csv");
            Console.WriteLine();
            Console.WriteLine("If output paths are not provided, default names will be used:");
            Console.WriteLine("  <logfile>_summary.txt");
            Console.WriteLine("  <logfile>_summary.csv");
            Console.WriteLine();
        }
    }
}
