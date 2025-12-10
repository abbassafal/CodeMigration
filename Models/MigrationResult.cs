using System;
using System.Collections.Generic;

namespace DataMigration.Models
{
    public class MigrationResult
    {
        public int InsertedCount { get; set; }
        public int SkippedCount { get; set; }
        public Dictionary<string, int> SkipReasons { get; set; } = new Dictionary<string, int>();
        public List<string> Logs { get; set; } = new List<string>();
        public int TotalProcessed => InsertedCount + SkippedCount;
        
        public void AddLog(string logMessage)
        {
            Logs.Add($"[{DateTime.Now:HH:mm:ss}] {logMessage}");
        }
        
        public void AddSkipReason(string reason)
        {
            if (SkipReasons.ContainsKey(reason))
                SkipReasons[reason]++;
            else
                SkipReasons[reason] = 1;
            
            SkippedCount++;
        }
        
        public string GetSummary()
        {
            var summary = $"Total Processed: {TotalProcessed}, Inserted: {InsertedCount}, Skipped: {SkippedCount}";
            if (SkipReasons.Count > 0)
            {
                summary += "\nSkip Reasons:";
                foreach (var reason in SkipReasons)
                {
                    summary += $"\n  - {reason.Key}: {reason.Value} records";
                }
            }
            return summary;
        }
    }
}
