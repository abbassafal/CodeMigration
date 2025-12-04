using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;

public class EventScheduleHistoryMigrationService
{
    private readonly ILogger<EventScheduleHistoryMigrationService> _logger;
    private readonly IConfiguration _configuration;

    public EventScheduleHistoryMigrationService(IConfiguration configuration, ILogger<EventScheduleHistoryMigrationService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "N/A", target = "history_id", type = "Auto-generated serial -> integer" },
            new { source = "EVENTSCHEDULARID", target = "event_schedule_id", type = "int -> integer" },
            new { source = "EVENTID", target = "event_id", type = "int -> integer" },
            new { source = "TIMEZONE", target = "time_zone", type = "nvarchar -> character varying" },
            new { source = "TIMEZONECOUNTRY", target = "time_country", type = "nvarchar -> character varying" },
            new { source = "OPENDATETIME", target = "event_start_date_time", type = "datetime -> timestamp with time zone" },
            new { source = "CLOSEDATETIME", target = "event_end_date_time", type = "datetime -> timestamp with time zone" },
            new { source = "REASONID", target = "event_schedule_change_remark", type = "int -> character varying (converted via join on TBL_REASONMASTER)" },
            new { source = "N/A", target = "operation_type", type = "character varying (default: 'INSERT')" },
            new { source = "default: 'INSERT'", target = "operation_type", type = "character varying (for audit trail)" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        int migratedCount = 0;
        int skippedCount = 0;
        var skippedEventIds = new List<int>();
        
        using var sqlConnection = new SqlConnection(_configuration.GetConnectionString("SqlServer"));
        using var pgConnection = new NpgsqlConnection(_configuration.GetConnectionString("PostgreSql"));
        await sqlConnection.OpenAsync();
        await pgConnection.OpenAsync();

        // Get all valid event_ids from event_master in PostgreSQL
        var validEventIds = new HashSet<int>();
        using (var checkCmd = new NpgsqlCommand("SELECT event_id FROM event_master", pgConnection))
        {
            using var checkReader = await checkCmd.ExecuteReaderAsync();
            while (await checkReader.ReadAsync())
            {
                validEventIds.Add(checkReader.GetInt32(0));
            }
        }
        
        _logger.LogInformation($"Found {validEventIds.Count} valid event IDs in event_master table.");

        // Query with join to get reason text from REASONID
        var selectQuery = @"
            SELECT 
                e.EVENTSCHEDULARID,
                e.EVENTID,
                e.TIMEZONE,
                e.TIMEZONECOUNTRY,
                e.OPENDATETIME,
                e.CLOSEDATETIME,
                ISNULL(r.Reason, 'Historical Record') AS ReasonRemark
            FROM TBL_EVENTSCHEDULAR e
            LEFT JOIN TBL_REASONMASTER r ON e.REASONID = r.ReasonID
            ORDER BY e.EVENTSCHEDULARID";
        
        using var sqlCmd = new SqlCommand(selectQuery, sqlConnection);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        var copyCommand = @"COPY event_schedule_history (
            event_schedule_id, event_id, time_zone, time_country,
            event_start_date_time, event_end_date_time, event_schedule_change_remark,
            operation_type
        ) FROM STDIN (FORMAT TEXT, DELIMITER '|')";

        using var writer = await pgConnection.BeginTextImportAsync(copyCommand);
        var now = DateTime.UtcNow;

        while (await reader.ReadAsync())
        {
            var eventId = reader["EVENTID"] != DBNull.Value ? Convert.ToInt32(reader["EVENTID"]) : 0;
            
            // Skip if event_id doesn't exist in event_master
            if (!validEventIds.Contains(eventId))
            {
                skippedCount++;
                skippedEventIds.Add(eventId);
                _logger.LogWarning($"Skipping event_schedule_history record {reader["EVENTSCHEDULARID"]} - event_id {eventId} not found in event_master");
                continue;
            }
            
            var fields = new string[]
            {
                FormatInteger(reader["EVENTSCHEDULARID"]),        // event_schedule_id (integer, NOT NULL)
                FormatInteger(reader["EVENTID"]),                  // event_id (integer, NOT NULL)
                FormatText(reader["TIMEZONE"]),                    // time_zone (character varying, NOT NULL)
                FormatText(reader["TIMEZONECOUNTRY"]),             // time_country (character varying, NOT NULL)
                FormatTimestamp(reader["OPENDATETIME"]),           // event_start_date_time (timestamp, NOT NULL)
                FormatTimestamp(reader["CLOSEDATETIME"]),          // event_end_date_time (timestamp, NOT NULL)
                FormatText(reader["ReasonRemark"]),                // event_schedule_change_remark (character varying, NOT NULL)
                "INSERT"                                          // operation_type (character varying)
            };
            var row = string.Join("|", fields);
            await writer.WriteLineAsync(row);
            migratedCount++;
        }
        
        writer.Close();
        
        if (skippedCount > 0)
        {
            _logger.LogWarning($"Migration completed with {skippedCount} skipped records (missing event_id in event_master).");
            _logger.LogWarning($"Skipped event_ids: {string.Join(", ", skippedEventIds.Distinct().OrderBy(x => x))}");
        }
        
        _logger.LogInformation($"Migrated {migratedCount} event_schedule_history records. Skipped {skippedCount} records.");
        return migratedCount;
    }

    // Helper method for integer fields (NOT NULL columns)
    private static string FormatInteger(object? value)
    {
        if (value == null || value == DBNull.Value)
            return "0";
        
        var str = value.ToString();
        if (string.IsNullOrWhiteSpace(str))
            return "0";
        
        return str;
    }

    // Helper method for text fields (NOT NULL columns)
    private static string FormatText(object? value)
    {
        if (value == null || value == DBNull.Value)
            return "Historical Record";  // Default value for NOT NULL text fields
        
        var str = value.ToString();
        if (string.IsNullOrEmpty(str))
            return "Historical Record";
        
        // Escape special characters for COPY TEXT format with pipe delimiter
        return str
            .Replace("\\", "\\\\")   // Backslash must be first
            .Replace("|", "\\|")     // Pipe (our delimiter)
            .Replace("\t", "\\t")    // Tab
            .Replace("\n", "\\n")    // Newline
            .Replace("\r", "\\r");   // Carriage return
    }

    // Helper method for timestamp fields (NOT NULL columns)
    private static string FormatTimestamp(object? value)
    {
        if (value == null || value == DBNull.Value)
            return DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00");
        
        if (value is DateTime dt)
        {
            var utcDt = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            return utcDt.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00");
        }
        
        // Try to parse string to datetime
        if (DateTime.TryParse(value.ToString(), out DateTime parsedDt))
        {
            var utcDt = parsedDt.Kind == DateTimeKind.Utc ? parsedDt : DateTime.SpecifyKind(parsedDt, DateTimeKind.Utc);
            return utcDt.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00");
        }
        
        return DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00");
    }
}
