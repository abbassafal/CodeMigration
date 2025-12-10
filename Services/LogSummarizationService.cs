using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DataMigration.Services
{
    public class LogSummarizationService
    {
        public class LogSummary
        {
            public string TableName { get; set; } = string.Empty;
            public List<GroupedLogEntry> GroupedLogs { get; set; } = new List<GroupedLogEntry>();
            public int TotalLines { get; set; }
            public int GroupedLines { get; set; }
        }

        public class GroupedLogEntry
        {
            public string LogPattern { get; set; } = string.Empty;
            public string LogLevel { get; set; } = string.Empty;
            public List<string> RecordIds { get; set; } = new List<string>();
            public int Count { get; set; }
            public string FirstTimestamp { get; set; } = string.Empty;
            public string LastTimestamp { get; set; } = string.Empty;
            public string IdRange { get; set; } = string.Empty;
        }

        public LogSummary SummarizeLogFile(string logFilePath)
        {
            if (!File.Exists(logFilePath))
            {
                throw new FileNotFoundException($"Log file not found: {logFilePath}");
            }

            var lines = File.ReadAllLines(logFilePath);
            return SummarizeLogLines(lines);
        }

        public LogSummary SummarizeLogLines(string[] lines)
        {
            var summary = new LogSummary
            {
                TotalLines = lines.Length
            };

            // Parse all log lines - handling multi-line format
            var allLogs = new List<LogLine>();
            LogLine? currentLogLine = null;
            
            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                if (string.IsNullOrWhiteSpace(line)) continue;

                // Check if this is a header line (info:, warn:, error:, etc.)
                if (Regex.IsMatch(line, @"^(info|warn|error|fail|debug):\s+", RegexOptions.IgnoreCase))
                {
                    // Save previous log line if exists
                    if (currentLogLine != null && !string.IsNullOrEmpty(currentLogLine.Message))
                    {
                        allLogs.Add(currentLogLine);
                    }
                    
                    // Parse new log header
                    currentLogLine = ParseLogHeader(line);
                }
                else if (line.StartsWith("      ") && currentLogLine != null)
                {
                    // This is a continuation line - append to current log
                    var message = line.Trim();
                    if (!string.IsNullOrEmpty(message))
                    {
                        currentLogLine.Message = message;
                        currentLogLine.Pattern = CreatePattern(message);
                        ExtractRecordId(currentLogLine);
                    }
                }
                else
                {
                    // Try parsing as standalone log line (bracket format)
                    var logLine = ParseStandaloneLogLine(line);
                    if (logLine != null)
                    {
                        if (currentLogLine != null && !string.IsNullOrEmpty(currentLogLine.Message))
                        {
                            allLogs.Add(currentLogLine);
                        }
                        allLogs.Add(logLine);
                        currentLogLine = null;
                    }
                }
            }
            
            // Don't forget the last log line
            if (currentLogLine != null && !string.IsNullOrEmpty(currentLogLine.Message))
            {
                allLogs.Add(currentLogLine);
            }

            // Group by table and then by pattern
            var tableGroups = allLogs.GroupBy(l => l.TableName);
            
            foreach (var tableGroup in tableGroups)
            {
                // Group similar log patterns within each table
                var patternGroups = tableGroup.GroupBy(l => $"{l.LogLevel}|{l.Pattern}");
                
                foreach (var pattern in patternGroups)
                {
                    var logs = pattern.ToList();
                    var firstLog = logs.First();
                    var groupedEntry = new GroupedLogEntry
                    {
                        LogPattern = firstLog.Message,
                        LogLevel = firstLog.LogLevel,
                        Count = logs.Count,
                        RecordIds = logs.Select(l => l.RecordId).Where(id => !string.IsNullOrEmpty(id)).Distinct().ToList(),
                        FirstTimestamp = logs.First().Timestamp,
                        LastTimestamp = logs.Last().Timestamp
                    };

                    // Generate ID range
                    groupedEntry.IdRange = GenerateIdRange(groupedEntry.RecordIds);
                    
                    // Replace actual IDs in pattern with placeholder
                    groupedEntry.LogPattern = Regex.Replace(groupedEntry.LogPattern, @"\d+", "{N}");
                    
                    summary.GroupedLogs.Add(groupedEntry);
                }
            }

            summary.GroupedLines = summary.GroupedLogs.Count;
            return summary;
        }
        
        private LogLine? ParseLogHeader(string line)
        {
            // Pattern: info: TableName[0] or info: Namespace.TableName[0]
            var match = Regex.Match(line, @"^(info|warn|error|fail|debug):\s+([^\[]+)\[(\d+)\]", RegexOptions.IgnoreCase);
            
            if (match.Success)
            {
                var fullName = match.Groups[2].Value.Trim();
                // Extract just the class name from full namespace (e.g., "DataMigration.Services.DatabaseTruncateService" -> "DatabaseTruncateService")
                var tableName = fullName.Contains('.') ? fullName.Substring(fullName.LastIndexOf('.') + 1) : fullName;
                
                return new LogLine
                {
                    LogLevel = match.Groups[1].Value.ToUpper(),
                    TableName = tableName,
                    Message = "", // Will be filled by continuation line
                    Timestamp = ""
                };
            }
            
            return null;
        }
        
        private LogLine? ParseStandaloneLogLine(string line)
        {
            // Pattern 1: [INFO] 2024-12-10 10:00:01 - Table: MaterialMaster - Successfully migrated record with ID: 1
            var match = Regex.Match(line, @"^\[(\w+)\]\s+([\d-]+\s+[\d:]+)\s+-\s+Table:\s+(\w+)\s+-\s+(.+)$", RegexOptions.IgnoreCase);
            
            if (match.Success)
            {
                var logLine = new LogLine
                {
                    LogLevel = match.Groups[1].Value.ToUpper(),
                    Timestamp = match.Groups[2].Value.Trim(),
                    TableName = match.Groups[3].Value.Trim(),
                    Message = match.Groups[4].Value.Trim()
                };

                ExtractRecordId(logLine);
                logLine.Pattern = CreatePattern(logLine.Message);
                return logLine;
            }

            // Pattern 2: [INFO] 2024-12-10 10:00:01 - Migrated 500 records for EventItems
            var altMatch = Regex.Match(line, @"^\[(\w+)\]\s+([\d-]+\s+[\d:]+)\s+-\s+(.+)$", RegexOptions.IgnoreCase);
            
            if (altMatch.Success)
            {
                var message = altMatch.Groups[3].Value.Trim();
                var logLine = new LogLine
                {
                    LogLevel = altMatch.Groups[1].Value.ToUpper(),
                    Timestamp = altMatch.Groups[2].Value.Trim(),
                    TableName = ExtractTableNameFromMessage(message),
                    Message = message
                };

                ExtractRecordId(logLine);
                logLine.Pattern = CreatePattern(logLine.Message);
                return logLine;
            }

            return null;
        }

        private class LogLine
        {
            public string Timestamp { get; set; } = string.Empty;
            public string LogLevel { get; set; } = string.Empty;
            public string TableName { get; set; } = string.Empty;
            public string Message { get; set; } = string.Empty;
            public string RecordId { get; set; } = string.Empty;
            public string Pattern { get; set; } = string.Empty;
        }

        private LogLine? ParseLogLine(string line)
        {
            // Pattern 1: [INFO] 2024-12-10 10:00:01 - Table: MaterialMaster - Successfully migrated record with ID: 1
            var match = Regex.Match(line, @"^\[(\w+)\]\s+([\d-]+\s+[\d:]+)\s+-\s+Table:\s+(\w+)\s+-\s+(.+)$", RegexOptions.IgnoreCase);
            
            if (match.Success)
            {
                var logLine = new LogLine
                {
                    LogLevel = match.Groups[1].Value.ToUpper(),
                    Timestamp = match.Groups[2].Value.Trim(),
                    TableName = match.Groups[3].Value.Trim(),
                    Message = match.Groups[4].Value.Trim()
                };

                ExtractRecordId(logLine);
                logLine.Pattern = CreatePattern(logLine.Message);
                return logLine;
            }

            // Pattern 2: [INFO] 2024-12-10 10:00:01 - Migrated 500 records for EventItems
            var altMatch = Regex.Match(line, @"^\[(\w+)\]\s+([\d-]+\s+[\d:]+)\s+-\s+(.+)$", RegexOptions.IgnoreCase);
            
            if (altMatch.Success)
            {
                var message = altMatch.Groups[3].Value.Trim();
                var logLine = new LogLine
                {
                    LogLevel = altMatch.Groups[1].Value.ToUpper(),
                    Timestamp = altMatch.Groups[2].Value.Trim(),
                    TableName = ExtractTableNameFromMessage(message),
                    Message = message
                };

                ExtractRecordId(logLine);
                logLine.Pattern = CreatePattern(logLine.Message);
                return logLine;
            }

            // Pattern 3: info: TableName[0]
            //           Message content
            var infoMatch = Regex.Match(line, @"^(info|warn|error|fail|debug):\s+([^\[]+)\[(\d+)\]", RegexOptions.IgnoreCase);
            
            if (infoMatch.Success)
            {
                var logLine = new LogLine
                {
                    LogLevel = infoMatch.Groups[1].Value.ToUpper(),
                    TableName = infoMatch.Groups[2].Value.Trim(),
                    Message = line.Substring(infoMatch.Length).Trim(),
                    Timestamp = ""
                };

                ExtractRecordId(logLine);
                logLine.Pattern = CreatePattern(logLine.Message);
                return logLine;
            }

            // Pattern 4: Continuation lines (indented messages)
            // These are often multi-line log messages
            if (line.StartsWith("      ") && line.Trim().Length > 0)
            {
                var logLine = new LogLine
                {
                    LogLevel = "INFO",
                    TableName = "General",
                    Message = line.Trim(),
                    Timestamp = ""
                };

                ExtractRecordId(logLine);
                logLine.Pattern = CreatePattern(logLine.Message);
                return logLine;
            }

            return null;
        }

        private string ExtractTableNameFromMessage(string message)
        {
            // Try to extract table name from messages like "Migrated 500 records for EventItems"
            var match = Regex.Match(message, @"for\s+(\w+)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract from "Migration completed for MaterialMaster"
            match = Regex.Match(message, @"completed for\s+(\w+)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract from "in EventCommunication"
            match = Regex.Match(message, @"in\s+(\w+)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract from "[table_name]" format
            match = Regex.Match(message, @"\[([^\]]+)\]");
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract table name from "Truncated table: table_name"
            match = Regex.Match(message, @"Truncated table:\s+(\w+)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract from "Seeded N records into table_name"
            match = Regex.Match(message, @"into\s+(\w+)\s+table", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract from "Starting table_name migration"
            match = Regex.Match(message, @"Starting\s+(\w+)\s+migration", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            return "General";
        }

        private void ExtractRecordId(LogLine logLine)
        {
            // Try different ID extraction patterns
            var patterns = new[]
            {
                @"with ID:\s*(\d+)",           // "with ID: 1"
                @"record\s+(\d+)",             // "record 103"
                @"Record\s+(\d+)",             // "Record 300"
                @"ID:\s*(\d+)",                // "ID: 123"
                @"(\d+)\s+records",            // "500 records"
                @"Inserted:\s*(\d+)",          // "Inserted: 100"
                @"Processed\s+(\d+)",          // "Processed 10"
                @"Seeded\s+(\d+)",             // "Seeded 5 records"
                @"Found\s+(\d+)",              // "Found 129 tables"
                @"POConditionTypeId=(\d+)",    // "POConditionTypeId=1"
                @"ProcessedCount=(\d+)",       // "ProcessedCount=1"
                @"ClientSAPId[=:\s]+(\d+)",
                @"EventId[=:\s]+(\d+)",
                @"MaterialId[=:\s]+(\d+)",
                @"SupplierId[=:\s]+(\d+)",
                @"Id[=:\s]+(\d+)",
                @"\[(\d+)\]"
            };

            foreach (var pattern in patterns)
            {
                var match = Regex.Match(logLine.Message, pattern, RegexOptions.IgnoreCase);
                if (match.Success)
                {
                    logLine.RecordId = match.Groups[1].Value;
                    break;
                }
            }
        }

        private string CreatePattern(string message)
        {
            // Replace all numbers with {N}
            var pattern = Regex.Replace(message, @"\d+", "{N}");
            
            // Replace specific IDs with {ID}
            pattern = Regex.Replace(pattern, @"(ClientSAPId|EventId|MaterialId|SupplierId|Id)[=:\s]+\{N\}", "$1={ID}", RegexOptions.IgnoreCase);
            
            return pattern;
        }

        private Dictionary<string, List<LogLine>> GroupByPattern(List<LogLine> logLines)
        {
            return logLines.GroupBy(l => l.Pattern)
                          .ToDictionary(g => g.Key, g => g.ToList());
        }

        private string GenerateIdRange(List<string> recordIds)
        {
            if (recordIds == null || recordIds.Count == 0)
            {
                return "N/A";
            }

            if (recordIds.Count == 1)
            {
                return recordIds[0];
            }

            // Try to parse as numbers
            var numericIds = new List<int>();
            foreach (var id in recordIds)
            {
                if (int.TryParse(id, out int numId))
                {
                    numericIds.Add(numId);
                }
            }

            if (numericIds.Count == 0)
            {
                return $"{recordIds.First()}...{recordIds.Last()} ({recordIds.Count} items)";
            }

            numericIds.Sort();
            
            // Check if continuous
            bool isContinuous = true;
            for (int i = 1; i < numericIds.Count; i++)
            {
                if (numericIds[i] != numericIds[i - 1] + 1)
                {
                    isContinuous = false;
                    break;
                }
            }

            if (isContinuous && numericIds.Count > 2)
            {
                return $"{numericIds.First()}-{numericIds.Last()}";
            }
            else if (numericIds.Count > 5)
            {
                return $"{numericIds.First()}, {numericIds[1]}, ..., {numericIds[numericIds.Count - 2]}, {numericIds.Last()} ({numericIds.Count} items)";
            }
            else
            {
                return string.Join(", ", numericIds);
            }
        }

        public string GenerateTextReport(LogSummary summary)
        {
            var sb = new StringBuilder();
            sb.AppendLine("=== LOG SUMMARIZATION REPORT ===");
            sb.AppendLine($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"Total Lines: {summary.TotalLines}");
            sb.AppendLine($"Grouped Lines: {summary.GroupedLines}");
            sb.AppendLine($"Compression Ratio: {(summary.GroupedLines > 0 ? (double)summary.TotalLines / summary.GroupedLines : 0):F2}x");
            sb.AppendLine();

            // Group by table
            var tableGroups = summary.GroupedLogs.GroupBy(l => 
                ExtractTableName(l.LogPattern)).OrderBy(g => g.Key);

            foreach (var tableGroup in tableGroups)
            {
                sb.AppendLine($"=== TABLE: {tableGroup.Key} ===");
                sb.AppendLine();

                foreach (var log in tableGroup.OrderByDescending(l => l.Count))
                {
                    sb.AppendLine($"[{log.LogLevel}] {log.LogPattern}");
                    sb.AppendLine($"  Count: {log.Count}");
                    sb.AppendLine($"  IDs: {log.IdRange}");
                    sb.AppendLine($"  Time: {log.FirstTimestamp} → {log.LastTimestamp}");
                    sb.AppendLine();
                }
            }

            return sb.ToString();
        }

        public string GenerateCsvReport(LogSummary summary)
        {
            var sb = new StringBuilder();
            sb.AppendLine("Table,Log Level,Pattern,Count,ID Range,First Timestamp,Last Timestamp");

            var tableGroups = summary.GroupedLogs.GroupBy(l => 
                ExtractTableName(l.LogPattern)).OrderBy(g => g.Key);

            foreach (var tableGroup in tableGroups)
            {
                foreach (var log in tableGroup.OrderByDescending(l => l.Count))
                {
                    var pattern = log.LogPattern.Replace("\"", "\"\"");
                    var idRange = log.IdRange.Replace("\"", "\"\"");
                    
                    sb.AppendLine($"{tableGroup.Key},{log.LogLevel},\"{pattern}\",{log.Count},\"{idRange}\",{log.FirstTimestamp},{log.LastTimestamp}");
                }
            }

            return sb.ToString();
        }

        private string ExtractTableName(string pattern)
        {
            // Extract table name from patterns like "[currency_master]" or "currency_master"
            var match = Regex.Match(pattern, @"\[([^\]]+)\]");
            if (match.Success)
            {
                return match.Groups[1].Value;
            }

            // Try to extract from common patterns
            var words = pattern.Split(new[] { ' ', '_' }, StringSplitOptions.RemoveEmptyEntries);
            if (words.Length > 0)
            {
                return words[0];
            }

            return "Unknown";
        }
    }
}
