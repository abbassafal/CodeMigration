using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class MaterialGroupMasterMigration : MigrationService
{
    private readonly ILogger<MaterialGroupMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT MaterialGroupId, SAPClientId, MaterialGroupCode, MaterialGroupName, MaterialGroupDescription, IsActive FROM TBL_MaterialGroupMaster";
    protected override string InsertQuery => @"INSERT INTO material_group_master (material_group_id, company_id, material_group_code, material_group_name, material_group_description, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@material_group_id, @company_id, @material_group_code, @material_group_name, @material_group_description, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public MaterialGroupMasterMigration(IConfiguration configuration, ILogger<MaterialGroupMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "material_group_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // material_group_id
            "FK",               // company_id
            "Direct",           // material_group_code
            "Direct",           // material_group_name
            "Direct",           // material_group_description
            "Default: 0",       // created_by
            "Default: Now",     // created_date
            "Default: null",    // modified_by
            "Default: null",    // modified_date
            "IsActive->IsDeleted", // is_deleted (inverted from IsActive)
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
            var materialGroupId = reader["MaterialGroupId"];
            var recordId = $"ID={materialGroupId}";
            try
            {
                pgCmd.Parameters.Clear();
                pgCmd.Parameters.AddWithValue("@material_group_id", materialGroupId);
                pgCmd.Parameters.AddWithValue("@company_id", reader["SAPClientId"]);
                pgCmd.Parameters.AddWithValue("@material_group_code", reader["MaterialGroupCode"]);
                pgCmd.Parameters.AddWithValue("@material_group_name", reader["MaterialGroupName"]);
                pgCmd.Parameters.AddWithValue("@material_group_description", reader["MaterialGroupDescription"]);
                pgCmd.Parameters.AddWithValue("@created_by", 0);
                pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
                pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                bool isActive = (int)reader["IsActive"] == 1;
                pgCmd.Parameters.AddWithValue("@is_deleted", !isActive);
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
        _logger.LogInformation($"Material Group Master Migration completed. Inserted: {summary.TotalInserted}, Skipped: {summary.TotalSkipped}");
        MigrationStatsExporter.ExportToExcel(
            "MaterialGroupMasterMigration_Stats.xlsx",
            totalRecords,
            summary.TotalInserted,
            summary.TotalSkipped,
            _logger,
            skippedRecordDetails
        );
        return summary.TotalInserted;
    }
}