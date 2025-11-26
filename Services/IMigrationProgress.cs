using System;

namespace DataMigration.Services
{
    public interface IMigrationProgress
    {
        void ReportProgress(int processed, int total, string currentOperation, TimeSpan elapsed);
        void ReportCompleted(int totalProcessed, int totalInserted, TimeSpan totalTime);
        void ReportError(string error, int processedSoFar);
    }

    public class MigrationProgressInfo
    {
        public int ProcessedRecords { get; set; }
        public int TotalRecords { get; set; }
        public int InsertedRecords { get; set; }
        public int SkippedRecords { get; set; }
        public string CurrentOperation { get; set; } = "";
        public TimeSpan ElapsedTime { get; set; }
        public double ProgressPercentage => TotalRecords > 0 ? (double)ProcessedRecords / TotalRecords * 100 : 0;
        public double RecordsPerSecond => ElapsedTime.TotalSeconds > 0 ? ProcessedRecords / ElapsedTime.TotalSeconds : 0;
        public TimeSpan EstimatedTimeRemaining
        {
            get
            {
                if (ProcessedRecords == 0 || ElapsedTime.TotalSeconds == 0) return TimeSpan.Zero;
                var remaining = TotalRecords - ProcessedRecords;
                var secondsPerRecord = ElapsedTime.TotalSeconds / ProcessedRecords;
                return TimeSpan.FromSeconds(remaining * secondsPerRecord);
            }
        }
    }

    public class ConsoleMigrationProgress : IMigrationProgress
    {
        private readonly object _lock = new object();
        private DateTime _lastUpdate = DateTime.MinValue;
        private readonly TimeSpan _updateInterval = TimeSpan.FromMilliseconds(500); // Update every 500ms

        public void ReportProgress(int processed, int total, string currentOperation, TimeSpan elapsed)
        {
            lock (_lock)
            {
                var now = DateTime.Now;
                if (now - _lastUpdate < _updateInterval && processed < total)
                    return; // Throttle updates

                _lastUpdate = now;

                var percentage = total > 0 ? (double)processed / total * 100 : 0;
                var recordsPerSecond = elapsed.TotalSeconds > 0 ? processed / elapsed.TotalSeconds : 0;
                var eta = GetETA(processed, total, elapsed);

                Console.Clear();
                Console.WriteLine("=== Material Master Migration Progress ===");
                Console.WriteLine($"Progress: {processed:N0}/{total:N0} ({percentage:F1}%)");
                Console.WriteLine($"Speed: {recordsPerSecond:F0} records/second");
                Console.WriteLine($"Elapsed: {elapsed:hh\\:mm\\:ss}");
                Console.WriteLine($"ETA: {eta:hh\\:mm\\:ss}");
                Console.WriteLine($"Current: {currentOperation}");
                
                // Progress bar
                var barWidth = 50;
                var filledWidth = (int)(percentage / 100 * barWidth);
                var bar = new string('█', filledWidth) + new string('░', barWidth - filledWidth);
                Console.WriteLine($"[{bar}] {percentage:F1}%");
                Console.WriteLine();
            }
        }

        public void ReportCompleted(int totalProcessed, int totalInserted, TimeSpan totalTime)
        {
            Console.Clear();
            Console.WriteLine("=== Migration Completed ===");
            Console.WriteLine($"Total Processed: {totalProcessed:N0}");
            Console.WriteLine($"Total Inserted: {totalInserted:N0}");
            Console.WriteLine($"Skipped: {totalProcessed - totalInserted:N0}");
            Console.WriteLine($"Total Time: {totalTime:hh\\:mm\\:ss}");
            Console.WriteLine($"Average Speed: {(totalTime.TotalSeconds > 0 ? totalProcessed / totalTime.TotalSeconds : 0):F0} records/second");
        }

        public void ReportError(string error, int processedSoFar)
        {
            Console.WriteLine();
            Console.WriteLine($"ERROR after processing {processedSoFar:N0} records:");
            Console.WriteLine(error);
        }

        private TimeSpan GetETA(int processed, int total, TimeSpan elapsed)
        {
            if (processed == 0 || elapsed.TotalSeconds == 0) return TimeSpan.Zero;
            
            var remaining = total - processed;
            var secondsPerRecord = elapsed.TotalSeconds / processed;
            return TimeSpan.FromSeconds(remaining * secondsPerRecord);
        }
    }
}
