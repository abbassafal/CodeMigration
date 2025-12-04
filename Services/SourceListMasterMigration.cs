using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SourceListMasterMigration
    {
        private readonly ILogger<SourceListMasterMigration> _logger;
        private readonly IConfiguration _configuration;

        public SourceListMasterMigration(IConfiguration configuration, ILogger<SourceListMasterMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "InfoRecordId", target = "sourcelist_master_id", type = "int -> integer" },
                new { source = "ClientSAPId", target = "company_id", type = "int -> integer (FK to company_master, NOT NULL, default 0)" },
                new { source = "SAPInstance", target = "company_code", type = "nvarchar -> text" },
                new { source = "VendorId", target = "supplier_id", type = "int -> integer (FK to supplier_master)" },
                new { source = "VendorCode", target = "supplier_code", type = "nvarchar -> text" },
                new { source = "VendorName", target = "supplier_name", type = "nvarchar -> text" },
                new { source = "MaterialGrpId", target = "material_group_id", type = "int -> integer (FK to material_group_master, NOT NULL, default 0)" },
                new { source = "MaterialGrpCode", target = "material_group_code", type = "nvarchar -> text" },
                new { source = "MaterialGrpName", target = "material_group_name", type = "nvarchar -> text" },
                new { source = "ItemId", target = "material_id", type = "int -> integer (FK to material_master, NOT NULL, default 0)" },
                new { source = "ItemCode", target = "material_code", type = "nvarchar -> text" },
                new { source = "ItemDescription", target = "material_name", type = "nvarchar -> text" },
                new { source = "ItemLongDescription", target = "material_description", type = "nvarchar -> text" },
                new { source = "Default: 'Active'", target = "status", type = "NOT NULL, default 'Active'" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SourceListMaster migration...");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        InfoRecordId,
                        ClientSAPId,
                        SAPInstance,
                        VendorId,
                        VendorCode,
                        VendorName,
                        MaterialGrpId,
                        MaterialGrpCode,
                        MaterialGrpName,
                        ItemId,
                        ItemCode,
                        ItemDescription,
                        ItemLongDescription
                    FROM tbl_InfoRecord", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            InfoRecordId = reader.GetInt32(0),
                            ClientSAPId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            SAPInstance = reader.IsDBNull(2) ? null : reader.GetString(2),
                            VendorId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            VendorCode = reader.IsDBNull(4) ? null : reader.GetString(4),
                            VendorName = reader.IsDBNull(5) ? null : reader.GetString(5),
                            MaterialGrpId = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                            MaterialGrpCode = reader.IsDBNull(7) ? null : reader.GetString(7),
                            MaterialGrpName = reader.IsDBNull(8) ? null : reader.GetString(8),
                            ItemId = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            ItemCode = reader.IsDBNull(10) ? null : reader.GetString(10),
                            ItemDescription = reader.IsDBNull(11) ? null : reader.GetString(11),
                            ItemLongDescription = reader.IsDBNull(12) ? null : reader.GetString(12)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records from tbl_InfoRecord");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        var targetRow = new TargetRow
                        {
                            SourceListMasterId = record.InfoRecordId,
                            CompanyId = record.ClientSAPId ?? 0, // NOT NULL - default to 0
                            CompanyCode = record.SAPInstance,
                            SupplierId = record.VendorId,
                            SupplierCode = record.VendorCode,
                            SupplierName = record.VendorName,
                            MaterialGroupId = record.MaterialGrpId ?? 0, // NOT NULL - default to 0
                            MaterialGroupCode = record.MaterialGrpCode,
                            MaterialGroupName = record.MaterialGrpName,
                            MaterialId = record.ItemId ?? 0, // NOT NULL - default to 0
                            MaterialCode = record.ItemCode,
                            MaterialName = record.ItemDescription,
                            MaterialDescription = record.ItemLongDescription,
                            Status = "Active" // NOT NULL - default status
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"InfoRecordId {record.InfoRecordId}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            return migratedRecords;
        }

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO sourcelist_master (");
            sql.AppendLine("    sourcelist_master_id, company_id, company_code, supplier_id,");
            sql.AppendLine("    supplier_code, supplier_name, material_group_id, material_group_code,");
            sql.AppendLine("    material_group_name, material_id, material_code, material_name,");
            sql.AppendLine("    material_description, status, created_by, created_date,");
            sql.AppendLine("    modified_by, modified_date, is_deleted, deleted_by, deleted_date,");
            sql.AppendLine("    status_updated_on");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@SourceListMasterId{i}, @CompanyId{i}, @CompanyCode{i}, @SupplierId{i}, @SupplierCode{i}, @SupplierName{i}, @MaterialGroupId{i}, @MaterialGroupCode{i}, @MaterialGroupName{i}, @MaterialId{i}, @MaterialCode{i}, @MaterialName{i}, @MaterialDescription{i}, @Status{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@SourceListMasterId{i}", row.SourceListMasterId);
                cmd.Parameters.AddWithValue($"@CompanyId{i}", row.CompanyId);
                cmd.Parameters.AddWithValue($"@CompanyCode{i}", (object?)row.CompanyCode ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", (object?)row.SupplierId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierCode{i}", (object?)row.SupplierCode ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierName{i}", (object?)row.SupplierName ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@MaterialGroupId{i}", row.MaterialGroupId);
                cmd.Parameters.AddWithValue($"@MaterialGroupCode{i}", (object?)row.MaterialGroupCode ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@MaterialGroupName{i}", (object?)row.MaterialGroupName ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@MaterialId{i}", row.MaterialId);
                cmd.Parameters.AddWithValue($"@MaterialCode{i}", (object?)row.MaterialCode ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@MaterialName{i}", (object?)row.MaterialName ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@MaterialDescription{i}", (object?)row.MaterialDescription ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@Status{i}", row.Status);
            }

            sql.AppendLine(string.Join(",\n", values));
            cmd.CommandText = sql.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                _logger.LogDebug($"Batch inserted {rowsAffected} records");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Batch insert failed: {ex.Message}");
                throw;
            }
        }

        private class SourceRow
        {
            public int InfoRecordId { get; set; }
            public int? ClientSAPId { get; set; }
            public string? SAPInstance { get; set; }
            public int? VendorId { get; set; }
            public string? VendorCode { get; set; }
            public string? VendorName { get; set; }
            public int? MaterialGrpId { get; set; }
            public string? MaterialGrpCode { get; set; }
            public string? MaterialGrpName { get; set; }
            public int? ItemId { get; set; }
            public string? ItemCode { get; set; }
            public string? ItemDescription { get; set; }
            public string? ItemLongDescription { get; set; }
        }

        private class TargetRow
        {
            public int SourceListMasterId { get; set; }
            public int CompanyId { get; set; } // NOT NULL
            public string? CompanyCode { get; set; }
            public int? SupplierId { get; set; }
            public string? SupplierCode { get; set; }
            public string? SupplierName { get; set; }
            public int MaterialGroupId { get; set; } // NOT NULL
            public string? MaterialGroupCode { get; set; }
            public string? MaterialGroupName { get; set; }
            public int MaterialId { get; set; } // NOT NULL
            public string? MaterialCode { get; set; }
            public string? MaterialName { get; set; }
            public string? MaterialDescription { get; set; }
            public string Status { get; set; } = "Active"; // NOT NULL
        }
    }
}
