using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SupplierBankDetailsMigration
    {
        private readonly ILogger<SupplierBankDetailsMigration> _logger;
        private readonly IConfiguration _configuration;

        public SupplierBankDetailsMigration(IConfiguration configuration, ILogger<SupplierBankDetailsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "VendorBankId", target = "bank_deails_id", type = "int -> integer" },
                new { source = "VendorID", target = "supplier_id", type = "int -> integer (FK to supplier_master, NOT NULL)" },
                new { source = "Default: 1", target = "bank_country_id", type = "NOT NULL, default 1" },
                new { source = "Default: ''", target = "bank_key", type = "NOT NULL, default ''" },
                new { source = "BankName", target = "bank_name", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "AccountNo", target = "bank_account", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "Default: ''", target = "bank_acc_holder", type = "NOT NULL, default ''" },
                new { source = "SWIFTCode", target = "swiftiban_no_bic", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "Branch", target = "branch_name", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "Default: ''", target = "branch_place", type = "NOT NULL, default ''" },
                new { source = "Default: ''", target = "branch_city", type = "NOT NULL, default ''" },
                new { source = "IFSCCode", target = "branch_code", type = "varchar -> text (NOT NULL, default '')" }
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

                _logger.LogInformation("Starting SupplierBankDetails migration...");

                // Build valid supplier IDs lookup
                var validSupplierIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT supplier_id FROM supplier_master", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validSupplierIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {validSupplierIds.Count} valid supplier_ids");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        VendorBankId,
                        VendorID,
                        BankName,
                        Branch,
                        AccountNo,
                        IFSCCode,
                        SWIFTCode
                    FROM tbl_VendorBankDetail", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            VendorBankId = reader.GetInt32(0),
                            VendorID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            BankName = reader.IsDBNull(2) ? null : reader.GetString(2),
                            Branch = reader.IsDBNull(3) ? null : reader.GetString(3),
                            AccountNo = reader.IsDBNull(4) ? null : reader.GetString(4),
                            IFSCCode = reader.IsDBNull(5) ? null : reader.GetString(5),
                            SWIFTCode = reader.IsDBNull(6) ? null : reader.GetString(6)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from tbl_VendorBankDetail");

                // Insert into PostgreSQL with batch processing
                var batchSize = 500;
                for (int i = 0; i < sourceData.Count; i += batchSize)
                {
                    var batch = sourceData.Skip(i).Take(batchSize);
                    
                    foreach (var row in batch)
                    {
                        try
                        {
                            // Validate supplier_id (NOT NULL constraint)
                            if (!row.VendorID.HasValue || !validSupplierIds.Contains(row.VendorID.Value))
                            {
                                _logger.LogWarning($"Skipping VendorBankId {row.VendorBankId}: Invalid or missing supplier_id {row.VendorID}");
                                skippedRecords++;
                                continue;
                            }

                            using var insertCmd = new NpgsqlCommand(@"
                                INSERT INTO supplier_bank_deails (
                                    bank_deails_id,
                                    supplier_id,
                                    bank_country_id,
                                    bank_key,
                                    bank_name,
                                    bank_account,
                                    bank_acc_holder,
                                    swiftiban_no_bic,
                                    branch_name,
                                    branch_place,
                                    branch_city,
                                    branch_code
                                ) VALUES (
                                    @bank_deails_id,
                                    @supplier_id,
                                    @bank_country_id,
                                    @bank_key,
                                    @bank_name,
                                    @bank_account,
                                    @bank_acc_holder,
                                    @swiftiban_no_bic,
                                    @branch_name,
                                    @branch_place,
                                    @branch_city,
                                    @branch_code
                                )
                                ON CONFLICT (bank_deails_id) DO UPDATE SET
                                    supplier_id = EXCLUDED.supplier_id,
                                    bank_country_id = EXCLUDED.bank_country_id,
                                    bank_key = EXCLUDED.bank_key,
                                    bank_name = EXCLUDED.bank_name,
                                    bank_account = EXCLUDED.bank_account,
                                    bank_acc_holder = EXCLUDED.bank_acc_holder,
                                    swiftiban_no_bic = EXCLUDED.swiftiban_no_bic,
                                    branch_name = EXCLUDED.branch_name,
                                    branch_place = EXCLUDED.branch_place,
                                    branch_city = EXCLUDED.branch_city,
                                    branch_code = EXCLUDED.branch_code", pgConnection);

                            insertCmd.Parameters.AddWithValue("@bank_deails_id", row.VendorBankId);
                            insertCmd.Parameters.AddWithValue("@supplier_id", row.VendorID.Value);
                            insertCmd.Parameters.AddWithValue("@bank_country_id", 1); // Default value
                            insertCmd.Parameters.AddWithValue("@bank_key", "");
                            insertCmd.Parameters.AddWithValue("@bank_name", row.BankName ?? "");
                            insertCmd.Parameters.AddWithValue("@bank_account", row.AccountNo ?? "");
                            insertCmd.Parameters.AddWithValue("@bank_acc_holder", "");
                            insertCmd.Parameters.AddWithValue("@swiftiban_no_bic", row.SWIFTCode ?? "");
                            insertCmd.Parameters.AddWithValue("@branch_name", row.Branch ?? "");
                            insertCmd.Parameters.AddWithValue("@branch_place", "");
                            insertCmd.Parameters.AddWithValue("@branch_city", "");
                            insertCmd.Parameters.AddWithValue("@branch_code", row.IFSCCode ?? "");

                            await insertCmd.ExecuteNonQueryAsync();
                            migratedRecords++;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError($"Error migrating VendorBankId {row.VendorBankId}: {ex.Message}");
                            skippedRecords++;
                        }
                    }

                    _logger.LogInformation($"Processed {Math.Min(i + batchSize, sourceData.Count)}/{sourceData.Count} records");
                }

                _logger.LogInformation($"SupplierBankDetails migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error during SupplierBankDetails migration: {ex.Message}");
                throw;
            }

            return migratedRecords;
        }

        private class SourceRow
        {
            public int VendorBankId { get; set; }
            public int? VendorID { get; set; }
            public string? BankName { get; set; }
            public string? Branch { get; set; }
            public string? AccountNo { get; set; }
            public string? IFSCCode { get; set; }
            public string? SWIFTCode { get; set; }
        }
    }
}
