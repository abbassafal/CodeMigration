using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;

public class EventScheduleMigrationService
{
    private readonly ILogger<EventScheduleMigrationService> _logger;
    private readonly IConfiguration _configuration;

    public EventScheduleMigrationService(IConfiguration configuration, ILogger<EventScheduleMigrationService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "EVENTSCHEDULARID", target = "event_schedule_id", type = "int -> integer" },
            new { source = "EVENTID", target = "event_id", type = "int -> integer" },
            new { source = "TIMEZONE", target = "time_zone", type = "nvarchar -> character varying" },
            new { source = "TIMEZONECOUNTRY", target = "time_country", type = "nvarchar -> character varying" },
            new { source = "OPENDATETIME", target = "event_start_date_time", type = "datetime -> timestamp with time zone" },
            new { source = "CLOSEDATETIME", target = "event_end_date_time", type = "datetime -> timestamp with time zone" },
            new { source = "AUCTION_START_DATE_TIME", target = "event_schedule_change_remark", type = "datetime -> character varying" },
            new { source = "AUCTION_END_DATE_TIME", target = "N/A", type = "datetime -> not mapped" },
            new { source = "TECHNICALSUBMISSIONDATE", target = "N/A", type = "datetime -> not mapped" },
            new { source = "REASONID", target = "N/A", type = "int -> not mapped" },
            new { source = "ENTERBY", target = "N/A", type = "int -> not mapped" },
            new { source = "ENTERDATE", target = "N/A", type = "datetime -> not mapped" },
            new { source = "RefEventId", target = "N/A", type = "int -> not mapped" },
            new { source = "LotNo", target = "N/A", type = "int -> not mapped" },
            new { source = "IsCompleted", target = "N/A", type = "int -> not mapped" },
            new { source = "default: 0", target = "created_by", type = "integer" },
            new { source = "default: UTC now", target = "created_date", type = "timestamp with time zone" },
            new { source = "default: NULL", target = "modified_by", type = "integer (nullable)" },
            new { source = "default: NULL", target = "modified_date", type = "timestamp with time zone (nullable)" },
            new { source = "default: NULL", target = "deleted_by", type = "integer (nullable)" },
            new { source = "default: NULL", target = "deleted_date", type = "timestamp with time zone (nullable)" },
            new { source = "default: false", target = "is_deleted", type = "boolean" }
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

        var selectQuery = @"
            SELECT 
                EVENTSCHEDULARID,
                EVENTID,
                TIMEZONE,
                TIMEZONECOUNTRY,
                OPENDATETIME,
                CLOSEDATETIME,
                AUCTION_START_DATE_TIME
            FROM TBL_EVENTSCHEDULAR 
            ORDER BY EVENTSCHEDULARID";
        
        using var sqlCmd = new SqlCommand(selectQuery, sqlConnection);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        var copyCommand = @"COPY event_schedule (
            event_schedule_id, event_id, time_zone, time_country,
            event_start_date_time, event_end_date_time, event_schedule_change_remark,
            created_by, created_date, modified_by, modified_date, 
            deleted_by, deleted_date, is_deleted
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
                _logger.LogWarning($"Skipping event_schedule_id {reader["EVENTSCHEDULARID"]} - event_id {eventId} not found in event_master");
                continue;
            }
            
            var fields = new string[]
            {
                FormatInteger(reader["EVENTSCHEDULARID"]),        // event_schedule_id (integer, NOT NULL)
                FormatInteger(reader["EVENTID"]),                  // event_id (integer, NOT NULL, FK)
                FormatText(reader["TIMEZONE"]),                    // time_zone (character varying, NOT NULL)
                FormatText(reader["TIMEZONECOUNTRY"]),             // time_country (character varying, NOT NULL)
                FormatTimestamp(reader["OPENDATETIME"]),           // event_start_date_time (timestamp, NOT NULL)
                FormatTimestamp(reader["CLOSEDATETIME"]),          // event_end_date_time (timestamp, NOT NULL)
                FormatText(reader["AUCTION_START_DATE_TIME"]),     // event_schedule_change_remark (character varying, NOT NULL)
                "0",                                               // created_by (integer)
                now.ToString("yyyy-MM-dd HH:mm:ss.ffffff+00"),     // created_date (timestamp)
                @"\N",                                             // modified_by (integer, nullable)
                @"\N",                                             // modified_date (timestamp, nullable)
                @"\N",                                             // deleted_by (integer, nullable)
                @"\N",                                             // deleted_date (timestamp, nullable)
                "f"                                                // is_deleted (boolean)
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
        
        _logger.LogInformation($"Migrated {migratedCount} event_schedule records. Skipped {skippedCount} records.");
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
            return "";
        
        var str = value.ToString();
        if (string.IsNullOrEmpty(str))
            return "";
        
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
