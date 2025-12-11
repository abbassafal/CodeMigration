using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class IncotermMasterMigration : MigrationService
{
    private readonly ILogger<IncotermMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT IncoID, IncoCode, IncoDescription, ClientSAPId FROM TBL_IncotermMASTER";
    
    protected override string InsertQuery => @"INSERT INTO incoterm_master (incoterm_id, incoterm_code, incoterm_name, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@incoterm_id, @incoterm_code, @incoterm_name, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public IncotermMasterMigration(IConfiguration configuration, ILogger<IncotermMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "incoterm_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // incoterm_id (IncoID)
            "Direct",           // incoterm_code (IncoCode)
            "Direct",           // incoterm_name (IncoDescription)
            "Direct",           // company_id (ClientSAPId)
            "Default: 0",       // created_by
            "Default: Now",     // created_date
            "Default: null",    // modified_by
            "Default: null",    // modified_date
            "Default: false",   // is_deleted
            "Default: null",    // deleted_by
            "Default: null"     // deleted_date
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();
        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }
        var skippedRecordDetails = new List<(string RecordId, string Reason)>();
        int totalRecords = 0;
        while (await reader.ReadAsync())
        {
            totalRecords++;
            var incoId = reader["IncoID"];
            var recordId = $"ID={incoId}";
            try
            {
                pgCmd.Parameters.Clear();
                pgCmd.Parameters.AddWithValue("@incoterm_id", incoId);
                pgCmd.Parameters.AddWithValue("@incoterm_code", reader["IncoCode"]);
                pgCmd.Parameters.AddWithValue("@incoterm_name", reader["IncoDescription"]);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
                pgCmd.Parameters.AddWithValue("@created_by", 0);
                pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
                pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@is_deleted", false);
                pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
                int result = await pgCmd.ExecuteNonQueryAsync();
                if (result > 0)
                {
                    migrationLogger.LogInserted(recordId);
                }
                else
                {
                    migrationLogger.LogSkipped(recordId, "Insert returned 0");
                    skippedRecordDetails.Add((recordId, "Insert returned 0"));
                }
            }
            catch (Exception ex)
            {
                migrationLogger.LogSkipped(recordId, ex.Message);
                skippedRecordDetails.Add((recordId, ex.Message));
            }
        }
        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"Incoterm Master Migration completed. Inserted: {summary.TotalInserted}, Skipped: {summary.TotalSkipped}");
        MigrationStatsExporter.ExportToExcel(
            "IncotermMasterMigration_Stats.xlsx",
            totalRecords,
            summary.TotalInserted,
            summary.TotalSkipped,
            _logger,
            skippedRecordDetails
        );
        return summary.TotalInserted;
    }
}