using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class SupplierInactiveMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly Microsoft.Extensions.Logging.ILogger<SupplierInactiveMigration> _logger;
    protected override string SelectQuery => @"
        SELECT 
            vi.VendorInactiveId,
            vi.VendorId,
            vi.CompanyCode,
            vi.Inactive,
            vi.InactiveDate
        FROM TBL_VendorInactive vi
        WHERE EXISTS (
            SELECT 1 
            FROM TBL_VENDORMASTERNEW vm 
            WHERE vm.VendorId = vi.VendorId
        )
        ORDER BY vi.VendorInactiveId";

    protected override string InsertQuery => @"
        INSERT INTO supplier_inactive (
            supplier_inactive_id,
            supplier_id,
            plant_company_code,
            inactive,
            inactivedate,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_inactive_id,
            @supplier_id,
            @plant_company_code,
            @inactive,
            @inactivedate,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public SupplierInactiveMigration(IConfiguration configuration, Microsoft.Extensions.Logging.ILogger<SupplierInactiveMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "VendorInactiveId -> supplier_inactive_id (Direct)",
            "VendorId -> supplier_id (Direct)",
            "CompanyCode -> plant_company_code (Direct)",
            "Inactive -> inactive (Convert to boolean: 'Y'/'1' = true, else false)",
            "InactiveDate -> inactivedate (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "VendorInactiveId", logic = "VendorInactiveId -> supplier_inactive_id (Direct)", target = "supplier_inactive_id" },
            new { source = "VendorId", logic = "VendorId -> supplier_id (Direct)", target = "supplier_id" },
            new { source = "CompanyCode", logic = "CompanyCode -> plant_company_code (Direct)", target = "plant_company_code" },
            new { source = "Inactive", logic = "Inactive -> inactive (Convert to boolean: 'Y'/'1' = true, else false)", target = "inactive" },
            new { source = "InactiveDate", logic = "InactiveDate -> inactivedate (Direct)", target = "inactivedate" },
            new { source = "-", logic = "created_by -> 0 (Fixed)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        int insertedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var vendorId = reader["VendorId"];
            if (vendorId == DBNull.Value || Convert.ToInt32(vendorId) == 0)
                continue; // Skip records with VendorId = 0

            // Determine inactive boolean: treat 'x' (case-insensitive) as true, otherwise false
            string inactiveRaw = reader["Inactive"] == DBNull.Value ? string.Empty : reader["Inactive"].ToString() ?? string.Empty;
            bool inactiveBool = string.Equals(inactiveRaw.Trim(), "x", StringComparison.OrdinalIgnoreCase);

            var record = new Dictionary<string, object>
            {
                ["supplier_inactive_id"] = reader["VendorInactiveId"],
                ["supplier_id"] = vendorId,
                ["plant_company_code"] = reader["CompanyCode"] ?? (object)DBNull.Value,
                ["inactive"] = inactiveBool,
                ["inactivedate"] = reader["InactiveDate"] ?? (object)DBNull.Value,
                ["created_by"] = 0,
                ["created_date"] = DateTime.UtcNow,
                ["modified_by"] = DBNull.Value,
                ["modified_date"] = DBNull.Value,
                ["is_deleted"] = false,
                ["deleted_by"] = DBNull.Value,
                ["deleted_date"] = DBNull.Value
            };
            batch.Add(record);
            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
                batch.Clear();
            }
        }
        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
        }
        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}");
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction = null, int batchNumber = 0)
    {
        if (batch.Count == 0) return 0;
        // Build a single multi-row insert
        var columns = new List<string> {
            "supplier_inactive_id", "supplier_id", "plant_company_code", "inactive", "inactivedate",
            "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
        };
        var valueRows = new List<string>();
        var parameters = new List<Npgsql.NpgsqlParameter>();
        int paramIndex = 0;
        foreach (var record in batch)
        {
            var valuePlaceholders = new List<string>();
            foreach (var col in columns)
            {
                var paramName = $"@p{paramIndex}";
                valuePlaceholders.Add(paramName);
                parameters.Add(new Npgsql.NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }
        var sql = $"INSERT INTO supplier_inactive ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new Npgsql.NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());
        int count = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {count} records.");
        return count;
    }
    }

