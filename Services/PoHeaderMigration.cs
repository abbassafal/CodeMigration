using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class PoHeaderMigration
    {
        private readonly ILogger<PoHeaderMigration> _logger;
        private readonly IConfiguration _configuration;

        public PoHeaderMigration(IConfiguration configuration, ILogger<PoHeaderMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "POID", target = "po_header_id", type = "int -> integer" },
                new { source = "PONO", target = "po_number", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "PODate", target = "po_creation_date", type = "datetime -> timestamp with time zone" },
                new { source = "POType", target = "po_type", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "POValue", target = "po_total_value", type = "decimal -> numeric" },
                new { source = "VendorCode", target = "supplier_code", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "VendorName", target = "supplier_name", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "Lookup: SELECT full_name FROM users WHERE user_id = AssignBuyerId", target = "buyer_name", type = "Lookup -> text (NOT NULL, default '')" },
                new { source = "AssignBuyerId", target = "user_id", type = "int -> integer" },
                new { source = "VendorId", target = "supplier_id", type = "int -> integer" },
                new { source = "IsConfirm (0:No, 1:Yes)", target = "release_to_vendor", type = "int -> text (NOT NULL, default 'No')" },
                new { source = "Currency", target = "currency", type = "varchar -> text (NOT NULL, default '')" },
                new { source = "PurchasingGroup", target = "purchase_group", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "PurchasingOrg", target = "purchase_organization", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "Paymentterm", target = "paymen_term", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "Incoterms1", target = "inco_terms_1", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "Incoterms2", target = "inco_terms_2", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "POCreator", target = "erp_po_creator", type = "nvarchar -> text (NOT NULL, default '')" },
                new { source = "POHeaderText", target = "po_header_text", type = "nvarchar -> integer" },
                new { source = "ClientSAPId", target = "company_id", type = "int -> integer (NOT NULL, default 0)" },
                new { source = "SupplierSatisfactionSurvey", target = "supplier_satisfaction_survey", type = "int -> text (NOT NULL, default '')" },
                new { source = "Default: ''", target = "po_layout_name", type = "NOT NULL, default ''" },
                new { source = "Default: ''", target = "po_layout_path", type = "NOT NULL, default ''" }
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

                _logger.LogInformation("Starting PoHeader migration...");

                // Build lookup for buyer names from PostgreSQL users table
                var buyerNameMap = new Dictionary<int, string>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT user_id, full_name 
                    FROM users 
                    WHERE user_id IS NOT NULL AND full_name IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var userId = reader.GetInt32(0);
                        var fullName = reader.GetString(1);
                        buyerNameMap[userId] = fullName;
                    }
                }
                _logger.LogInformation($"Found {buyerNameMap.Count} buyer names from users table");

                // Fetch source data from TBL_POMain
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        POID,
                        PONO,
                        PODate,
                        POType,
                        POValue,
                        VendorCode,
                        VendorName,
                        AssignBuyerId,
                        VendorId,
                        IsConfirm,
                        Currency,
                        PurchasingGroup,
                        PurchasingOrg,
                        Paymentterm,
                        Incoterms1,
                        Incoterms2,
                        POCreator,
                        POHeaderText,
                        ClientSAPId,
                        SupplierSatisfactionSurvey
                    FROM TBL_POMain
                    WHERE POID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            POID = reader.GetInt32(0),
                            PONO = reader.IsDBNull(1) ? null : reader.GetString(1),
                            PODate = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                            POType = reader.IsDBNull(3) ? null : reader.GetString(3),
                            POValue = reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                            VendorCode = reader.IsDBNull(5) ? null : reader.GetString(5),
                            VendorName = reader.IsDBNull(6) ? null : reader.GetString(6),
                            AssignBuyerId = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            VendorId = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                            IsConfirm = reader.IsDBNull(9) ? null : reader.GetInt32(9),
                            Currency = reader.IsDBNull(10) ? null : reader.GetString(10),
                            PurchasingGroup = reader.IsDBNull(11) ? null : reader.GetString(11),
                            PurchasingOrg = reader.IsDBNull(12) ? null : reader.GetString(12),
                            Paymentterm = reader.IsDBNull(13) ? null : reader.GetString(13),
                            Incoterms1 = reader.IsDBNull(14) ? null : reader.GetString(14),
                            Incoterms2 = reader.IsDBNull(15) ? null : reader.GetString(15),
                            POCreator = reader.IsDBNull(16) ? null : reader.GetString(16),
                            POHeaderText = reader.IsDBNull(17) ? null : reader.GetString(17),
                            ClientSAPId = reader.IsDBNull(18) ? null : reader.GetInt32(18),
                            SupplierSatisfactionSurvey = reader.IsDBNull(19) ? null : reader.GetInt32(19)
                        });
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records from TBL_POMain");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Lookup buyer_name (NOT NULL - default to empty string if not found)
                        string buyerName = "";
                        if (record.AssignBuyerId.HasValue && buyerNameMap.TryGetValue(record.AssignBuyerId.Value, out var name))
                        {
                            buyerName = name;
                        }

                        // Map IsConfirm to release_to_vendor (NOT NULL - default to 'No')
                        string releaseToVendor = "No";
                        if (record.IsConfirm.HasValue)
                        {
                            releaseToVendor = record.IsConfirm.Value == 1 ? "Yes" : "No";
                        }

                        // Parse POHeaderText to integer (nullable)
                        int? poHeaderText = null;
                        if (!string.IsNullOrWhiteSpace(record.POHeaderText) && int.TryParse(record.POHeaderText, out var headerTextValue))
                        {
                            poHeaderText = headerTextValue;
                        }

                        var targetRow = new TargetRow
                        {
                            PoHeaderId = record.POID,
                            PoNumber = record.PONO ?? "", // NOT NULL - default to empty string
                            PoCreationDate = record.PODate,
                            PoType = record.POType ?? "", // NOT NULL - default to empty string
                            PoTotalValue = record.POValue,
                            SupplierCode = record.VendorCode ?? "", // NOT NULL - default to empty string
                            SupplierName = record.VendorName ?? "", // NOT NULL - default to empty string
                            BuyerName = buyerName, // NOT NULL - default to empty string
                            UserId = record.AssignBuyerId,
                            SupplierId = record.VendorId,
                            ReleaseToVendor = releaseToVendor, // NOT NULL - default to 'No'
                            Currency = record.Currency ?? "", // NOT NULL - default to empty string
                            PurchaseGroup = record.PurchasingGroup ?? "", // NOT NULL - default to empty string
                            PurchaseOrganization = record.PurchasingOrg ?? "", // NOT NULL - default to empty string
                            PaymenTerm = record.Paymentterm ?? "", // NOT NULL - default to empty string
                            IncoTerms1 = record.Incoterms1 ?? "", // NOT NULL - default to empty string
                            IncoTerms2 = record.Incoterms2 ?? "", // NOT NULL - default to empty string
                            ErpPoCreator = record.POCreator ?? "", // NOT NULL - default to empty string
                            PoHeaderText = poHeaderText,
                            CompanyId = record.ClientSAPId ?? 0, // NOT NULL - default to 0
                            SupplierSatisfactionSurvey = record.SupplierSatisfactionSurvey?.ToString() ?? "", // NOT NULL - default to empty string
                            PoLayoutName = "", // NOT NULL - insert blank
                            PoLayoutPath = "" // NOT NULL - insert blank
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"POID {record.POID}: {ex.Message}");
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
            sql.AppendLine("INSERT INTO po_header (");
            sql.AppendLine("    po_header_id, po_number, po_creation_date, po_type, po_total_value,");
            sql.AppendLine("    supplier_code, supplier_name, buyer_name, user_id, supplier_id,");
            sql.AppendLine("    release_to_vendor, currency, purchase_group, purchase_organization,");
            sql.AppendLine("    paymen_term, inco_terms_1, inco_terms_2, erp_po_creator,");
            sql.AppendLine("    po_header_text, company_id, supplier_satisfaction_survey,");
            sql.AppendLine("    po_layout_name, po_layout_path,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@PoHeaderId{i}, @PoNumber{i}, @PoCreationDate{i}, @PoType{i}, @PoTotalValue{i}, @SupplierCode{i}, @SupplierName{i}, @BuyerName{i}, @UserId{i}, @SupplierId{i}, @ReleaseToVendor{i}, @Currency{i}, @PurchaseGroup{i}, @PurchaseOrganization{i}, @PaymenTerm{i}, @IncoTerms1{i}, @IncoTerms2{i}, @ErpPoCreator{i}, @PoHeaderText{i}, @CompanyId{i}, @SupplierSatisfactionSurvey{i}, @PoLayoutName{i}, @PoLayoutPath{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@PoHeaderId{i}", row.PoHeaderId);
                cmd.Parameters.AddWithValue($"@PoNumber{i}", row.PoNumber);
                cmd.Parameters.AddWithValue($"@PoCreationDate{i}", (object?)row.PoCreationDate ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@PoType{i}", row.PoType);
                cmd.Parameters.AddWithValue($"@PoTotalValue{i}", (object?)row.PoTotalValue ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierCode{i}", row.SupplierCode);
                cmd.Parameters.AddWithValue($"@SupplierName{i}", row.SupplierName);
                cmd.Parameters.AddWithValue($"@BuyerName{i}", row.BuyerName);
                cmd.Parameters.AddWithValue($"@UserId{i}", (object?)row.UserId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", (object?)row.SupplierId ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@ReleaseToVendor{i}", row.ReleaseToVendor);
                cmd.Parameters.AddWithValue($"@Currency{i}", row.Currency);
                cmd.Parameters.AddWithValue($"@PurchaseGroup{i}", row.PurchaseGroup);
                cmd.Parameters.AddWithValue($"@PurchaseOrganization{i}", row.PurchaseOrganization);
                cmd.Parameters.AddWithValue($"@PaymenTerm{i}", row.PaymenTerm);
                cmd.Parameters.AddWithValue($"@IncoTerms1{i}", row.IncoTerms1);
                cmd.Parameters.AddWithValue($"@IncoTerms2{i}", row.IncoTerms2);
                cmd.Parameters.AddWithValue($"@ErpPoCreator{i}", row.ErpPoCreator);
                cmd.Parameters.AddWithValue($"@PoHeaderText{i}", (object?)row.PoHeaderText ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@CompanyId{i}", row.CompanyId);
                cmd.Parameters.AddWithValue($"@SupplierSatisfactionSurvey{i}", row.SupplierSatisfactionSurvey);
                cmd.Parameters.AddWithValue($"@PoLayoutName{i}", row.PoLayoutName);
                cmd.Parameters.AddWithValue($"@PoLayoutPath{i}", row.PoLayoutPath);
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
            public int POID { get; set; }
            public string? PONO { get; set; }
            public DateTime? PODate { get; set; }
            public string? POType { get; set; }
            public decimal? POValue { get; set; }
            public string? VendorCode { get; set; }
            public string? VendorName { get; set; }
            public int? AssignBuyerId { get; set; }
            public int? VendorId { get; set; }
            public int? IsConfirm { get; set; }
            public string? Currency { get; set; }
            public string? PurchasingGroup { get; set; }
            public string? PurchasingOrg { get; set; }
            public string? Paymentterm { get; set; }
            public string? Incoterms1 { get; set; }
            public string? Incoterms2 { get; set; }
            public string? POCreator { get; set; }
            public string? POHeaderText { get; set; }
            public int? ClientSAPId { get; set; }
            public int? SupplierSatisfactionSurvey { get; set; }
        }

        private class TargetRow
        {
            public int PoHeaderId { get; set; }
            public string PoNumber { get; set; } = ""; // NOT NULL
            public DateTime? PoCreationDate { get; set; }
            public string PoType { get; set; } = ""; // NOT NULL
            public decimal? PoTotalValue { get; set; }
            public string SupplierCode { get; set; } = ""; // NOT NULL
            public string SupplierName { get; set; } = ""; // NOT NULL
            public string BuyerName { get; set; } = ""; // NOT NULL
            public int? UserId { get; set; }
            public int? SupplierId { get; set; }
            public string ReleaseToVendor { get; set; } = "No"; // NOT NULL
            public string Currency { get; set; } = ""; // NOT NULL
            public string PurchaseGroup { get; set; } = ""; // NOT NULL
            public string PurchaseOrganization { get; set; } = ""; // NOT NULL
            public string PaymenTerm { get; set; } = ""; // NOT NULL
            public string IncoTerms1 { get; set; } = ""; // NOT NULL
            public string IncoTerms2 { get; set; } = ""; // NOT NULL
            public string ErpPoCreator { get; set; } = ""; // NOT NULL
            public int? PoHeaderText { get; set; }
            public int CompanyId { get; set; } // NOT NULL
            public string SupplierSatisfactionSurvey { get; set; } = ""; // NOT NULL
            public string PoLayoutName { get; set; } = ""; // NOT NULL
            public string PoLayoutPath { get; set; } = ""; // NOT NULL
        }
    }
}
