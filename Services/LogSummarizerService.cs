using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataMigration.Services
{
    public class LogSummarizerService
    {
        public class LogEntry
        {
            public string? Timestamp { get; set; }
            public string LogLevel { get; set; } = string.Empty;
            public string Source { get; set; } = string.Empty;
            public string Message { get; set; } = string.Empty;
            public string OriginalLine { get; set; } = string.Empty;
            public List<string> ExtractedIds { get; set; } = new List<string>();
        }

        public class LogGroup
        {
            public string Pattern { get; set; } = string.Empty;
            public List<LogEntry> Entries { get; set; } = new List<LogEntry>();
            public int Count => Entries.Count;
            public string IdRange { get; set; } = string.Empty;
            public string? FirstTimestamp { get; set; }
            public string? LastTimestamp { get; set; }
        }

        public string SummarizeLogFile(string logFilePath, string? outputPath = null)
        {
            if (!File.Exists(logFilePath))
            {
                throw new FileNotFoundException($"Log file not found: {logFilePath}");
            }

            Console.WriteLine($"📂 Reading log file: {logFilePath}");
            var lines = File.ReadAllLines(logFilePath);
            Console.WriteLine($"📊 Total lines: {lines.Length:N0}");

            // Parse all log entries
            var logEntries = ParseLogEntries(lines);
            Console.WriteLine($"✓ Parsed {logEntries.Count:N0} log entries");

            // Group by pattern
            var groupedLogs = GroupLogsByPattern(logEntries);
            Console.WriteLine($"✓ Grouped into {groupedLogs.Count:N0} unique patterns");

            // Generate summary
            var summary = GenerateSummary(groupedLogs);

            // Save to file if output path provided
            if (outputPath != null)
            {
                File.WriteAllText(outputPath, summary);
                Console.WriteLine($"✓ Summary saved to: {outputPath}");
            }

            return summary;
        }

        private List<LogEntry> ParseLogEntries(string[] lines)
        {
            var entries = new List<LogEntry>();
            var logPattern = @"^(\w+):\s+([^\[]+)\[(\d+)\]\s*(.*)$";

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                var match = Regex.Match(line, logPattern);
                if (match.Success)
                {
                    var entry = new LogEntry
                    {
                        LogLevel = match.Groups[1].Value.Trim(),
                        Source = match.Groups[2].Value.Trim(),
                        Message = match.Groups[4].Value.Trim(),
                        OriginalLine = line
                    };

                    // Extract IDs from the message
                    entry.ExtractedIds = ExtractIds(entry.Message);
                    entries.Add(entry);
                }
            }

            return entries;
        }

        private List<string> ExtractIds(string message)
        {
            var ids = new List<string>();
            
            // Pattern for various ID formats
            var patterns = new List<string>
            {
                @"EventId[=:\s]+(\d+)",
                @"EventItemId[=:\s]+(\d+)",
                @"MaterialId[=:\s]+(\d+)",
                @"SupplierId[=:\s]+(\d+)",
                @"UserId[=:\s]+(\d+)",
                @"CompanyId[=:\s]+(\d+)",
                @"PlantId[=:\s]+(\d+)",
                @"Id[=:\s]+(\d+)",
                @"\b(\d{4,})\b" // Generic 4+ digit numbers
            };

            foreach (var pattern in patterns)
            {
                var matches = Regex.Matches(message, pattern);
                foreach (Match match in matches)
                {
                    if (match.Groups.Count > 1)
                    {
                        ids.Add(match.Groups[1].Value);
                    }
                }
            }

            return ids.Distinct().ToList();
        }

        private Dictionary<string, LogGroup> GroupLogsByPattern(List<LogEntry> entries)
        {
            var groups = new Dictionary<string, LogGroup>();

            foreach (var entry in entries)
            {
                // Create a pattern by removing IDs and numbers
                var pattern = NormalizePattern(entry.Message);
                var key = $"{entry.Source}||{pattern}";

                if (!groups.ContainsKey(key))
                {
                    groups[key] = new LogGroup
                    {
                        Pattern = pattern,
                        FirstTimestamp = entry.Timestamp
                    };
                }

                groups[key].Entries.Add(entry);
                groups[key].LastTimestamp = entry.Timestamp;
            }

            // Calculate ID ranges for each group
            foreach (var group in groups.Values)
            {
                group.IdRange = CalculateIdRange(group.Entries);
            }

            return groups;
        }

        private string NormalizePattern(string message)
        {
            // Replace specific IDs with placeholders
            var normalized = message;
            
            // Replace numbers with {N}
            normalized = Regex.Replace(normalized, @"\b\d+\b", "{N}");
            
            // Replace quoted strings with {STR}
            normalized = Regex.Replace(normalized, @"""[^""]*""", "{STR}");
            normalized = Regex.Replace(normalized, @"'[^']*'", "{STR}");
            
            // Normalize whitespace
            normalized = Regex.Replace(normalized, @"\s+", " ").Trim();

            return normalized;
        }

        private string CalculateIdRange(List<LogEntry> entries)
        {
            var allIds = entries
                .SelectMany(e => e.ExtractedIds)
                .Select(id => int.TryParse(id, out int num) ? num : -1)
                .Where(id => id > 0)
                .OrderBy(id => id)
                .ToList();

            if (allIds.Count == 0) return "N/A";
            if (allIds.Count == 1) return allIds[0].ToString();

            var ranges = new List<string>();
            int rangeStart = allIds[0];
            int rangeEnd = allIds[0];

            for (int i = 1; i < allIds.Count; i++)
            {
                if (allIds[i] == rangeEnd + 1)
                {
                    rangeEnd = allIds[i];
                }
                else
                {
                    // Add current range
                    if (rangeStart == rangeEnd)
                        ranges.Add(rangeStart.ToString());
                    else if (rangeEnd == rangeStart + 1)
                        ranges.Add($"{rangeStart}, {rangeEnd}");
                    else
                        ranges.Add($"{rangeStart}-{rangeEnd}");

                    rangeStart = allIds[i];
                    rangeEnd = allIds[i];
                }
            }

            // Add final range
            if (rangeStart == rangeEnd)
                ranges.Add(rangeStart.ToString());
            else if (rangeEnd == rangeStart + 1)
                ranges.Add($"{rangeStart}, {rangeEnd}");
            else
                ranges.Add($"{rangeStart}-{rangeEnd}");

            // If too many ranges, just show min-max
            if (ranges.Count > 5)
                return $"{allIds.Min()}-{allIds.Max()} ({allIds.Count} IDs)";

            return string.Join(", ", ranges);
        }

        private string GenerateSummary(Dictionary<string, LogGroup> groups)
        {
            var sb = new StringBuilder();
            
            sb.AppendLine("═══════════════════════════════════════════════════════════════════");
            sb.AppendLine("                    MIGRATION LOGS SUMMARY                         ");
            sb.AppendLine("═══════════════════════════════════════════════════════════════════");
            sb.AppendLine($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"Total Log Groups: {groups.Count:N0}");
            sb.AppendLine($"Total Log Entries: {groups.Values.Sum(g => g.Count):N0}");
            sb.AppendLine("═══════════════════════════════════════════════════════════════════\n");

            // Group by source
            var groupedBySource = groups.Values
                .GroupBy(g => g.Entries[0].Source)
                .OrderBy(g => g.Key);

            foreach (var sourceGroup in groupedBySource)
            {
                sb.AppendLine($"\n{'▼'} {sourceGroup.Key}");
                sb.AppendLine(new string('─', 70));

                var orderedGroups = sourceGroup
                    .OrderByDescending(g => g.Count)
                    .ToList();

                foreach (var group in orderedGroups)
                {
                    var firstEntry = group.Entries[0];
                    
                    sb.AppendLine($"\n  [{firstEntry.LogLevel}] Count: {group.Count:N0}");
                    
                    if (group.IdRange != "N/A")
                        sb.AppendLine($"  IDs: {group.IdRange}");
                    
                    sb.AppendLine($"  Pattern: {group.Pattern}");
                    
                    // Show first example
                    if (group.Count > 1)
                    {
                        sb.AppendLine($"  Example: {group.Entries[0].Message}");
                    }
                    else
                    {
                        sb.AppendLine($"  Message: {group.Entries[0].Message}");
                    }
                }
            }

            // Add statistics section
            sb.AppendLine("\n\n═══════════════════════════════════════════════════════════════════");
            sb.AppendLine("                         STATISTICS                                 ");
            sb.AppendLine("═══════════════════════════════════════════════════════════════════");

            var logLevelCounts = groups.Values
                .SelectMany(g => g.Entries)
                .GroupBy(e => e.LogLevel)
                .OrderByDescending(g => g.Count());

            foreach (var levelGroup in logLevelCounts)
            {
                sb.AppendLine($"  {levelGroup.Key}: {levelGroup.Count():N0}");
            }

            // Top patterns
            sb.AppendLine("\n\nTop 10 Most Frequent Patterns:");
            sb.AppendLine(new string('─', 70));
            
            var topPatterns = groups.Values
                .OrderByDescending(g => g.Count)
                .Take(10)
                .ToList();

            int rank = 1;
            foreach (var group in topPatterns)
            {
                sb.AppendLine($"{rank}. Count: {group.Count:N0} - {group.Pattern}");
                rank++;
            }

            return sb.ToString();
        }

        // Public method to generate CSV format
        public void GenerateCsvSummary(string logFilePath, string csvOutputPath)
        {
            var lines = File.ReadAllLines(logFilePath);
            var logEntries = ParseLogEntries(lines);
            var groupedLogs = GroupLogsByPattern(logEntries);

            var csv = new StringBuilder();
            csv.AppendLine("Source,Log Level,Count,ID Range,Pattern,Example Message");

            foreach (var group in groupedLogs.Values.OrderByDescending(g => g.Count))
            {
                var firstEntry = group.Entries[0];
                var escapedPattern = EscapeCsv(group.Pattern);
                var escapedExample = EscapeCsv(group.Entries[0].Message);

                csv.AppendLine($"{firstEntry.Source},{firstEntry.LogLevel},{group.Count},{group.IdRange},{escapedPattern},{escapedExample}");
            }

            File.WriteAllText(csvOutputPath, csv.ToString());
            Console.WriteLine($"✓ CSV summary saved to: {csvOutputPath}");
        }

        private string EscapeCsv(string value)
        {
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n"))
            {
                return $"\"{value.Replace("\"", "\"\"")}\"";
            }
            return value;
        }
    }
}
