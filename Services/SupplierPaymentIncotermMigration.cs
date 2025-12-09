using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading;
using System.Text;
using DataMigration.Services;

public class SupplierPaymentIncotermMigration : MigrationService
{
    private readonly ILogger<SupplierPaymentIncotermMigration>? _logger;
    private const int BATCH_SIZE = 1000; // Batch size for bulk inserts

    public SupplierPaymentIncotermMigration(IConfiguration configuration, ILogger<SupplierPaymentIncotermMigration>? logger = null) 
        : base(configuration) 
    { 
        _logger = logger;
    }
    
    protected override string SelectQuery => @"
        SELECT 
            VendorPOTermId,
            PurchaseOrgId,
            VendorId,
            PaymentTermId,
            IncoTermId,
            IncoTermRemark,
            ClientSAPId
        FROM TBL_VendorPOTerm
        ORDER BY VendorPOTermId";

    protected override string InsertQuery => @"
        INSERT INTO supplier_payment_incoterm (
            supplier_payment_incoterm_id,
            purchase_organization_id,
            supplier_id,
            payment_term_id,
            incoterm_id,
            incoterm_remark,
            company_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_payment_incoterm_id,
            @purchase_organization_id,
            @supplier_id,
            @payment_term_id,
            @incoterm_id,
            @incoterm_remark,
            @company_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "VendorPOTermId -> supplier_payment_incoterm (Direct)",
            "PurchaseOrgId -> purchase_organization_id (Direct)",
            "VendorId -> supplier_id (Direct)",
            "PaymentTermId -> payment_term_id (Direct)",
            "IncoTermId -> incoterm_id (Direct)",
            "IncoTermRemark -> incoterm_remark (Direct)",
            "ClientSAPId -> company_id (Direct)",
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
            new { source = "VendorPOTermId", logic = "VendorPOTermId -> supplier_payment_incoterm_id (Direct)", target = "supplier_payment_incoterm_id" },
            new { source = "PurchaseOrgId", logic = "PurchaseOrgId -> purchase_organization_id (Direct)", target = "purchase_organization_id" },
            new { source = "VendorId", logic = "VendorId -> supplier_id (Direct)", target = "supplier_id" },
            new { source = "PaymentTermId", logic = "PaymentTermId -> payment_term_id (Direct)", target = "payment_term_id" },
            new { source = "IncoTermId", logic = "IncoTermId -> incoterm_id (Direct)", target = "incoterm_id" },
            new { source = "IncoTermRemark", logic = "IncoTermRemark -> incoterm_remark (Direct)", target = "incoterm_remark" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Direct)", target = "company_id" },
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
        int skippedCount = 0;
        int totalCount = 0;
        var stopwatch = Stopwatch.StartNew();
        
        _logger?.LogInformation("🔍 Loading valid foreign key IDs from PostgreSQL...");
        
        // Load valid supplier IDs
        var validSupplierIds = new HashSet<int>();
        using (var cmd = new NpgsqlCommand("SELECT supplier_id FROM supplier_master WHERE is_deleted = false OR is_deleted IS NULL", pgConn, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                validSupplierIds.Add(reader.GetInt32(0));
            }
        }
        _logger?.LogInformation($"✓ Loaded {validSupplierIds.Count:N0} valid supplier IDs");
        
        // Load valid payment term IDs
        var validPaymentTermIds = new HashSet<int>();
        using (var cmd = new NpgsqlCommand("SELECT payment_term_id FROM payment_term_master WHERE is_deleted = false OR is_deleted IS NULL", pgConn, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                validPaymentTermIds.Add(reader.GetInt32(0));
            }
        }
        _logger?.LogInformation($"✓ Loaded {validPaymentTermIds.Count:N0} valid payment term IDs");
        
        // Load valid incoterm IDs
        var validIncotermIds = new HashSet<int>();
        using (var cmd = new NpgsqlCommand("SELECT incoterm_id FROM incoterm_master WHERE is_deleted = false OR is_deleted IS NULL", pgConn, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                validIncotermIds.Add(reader.GetInt32(0));
            }
        }
        _logger?.LogInformation($"✓ Loaded {validIncotermIds.Count:N0} valid incoterm IDs");
        
        _logger?.LogInformation("📖 Reading data from TBL_VendorPOTerm...");
        
        // Batch processing
        var batch = new List<SupplierPaymentIncotermRecord>();
        
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var sqlReader = await selectCmd.ExecuteReaderAsync();
        
        while (await sqlReader.ReadAsync())
        {
            totalCount++;
            
            try
            {
                var vendorPoTermId = Convert.ToInt32(sqlReader["VendorPOTermId"]);
                var purchaseOrgId = sqlReader.IsDBNull(sqlReader.GetOrdinal("PurchaseOrgId")) ? 0 : Convert.ToInt32(sqlReader["PurchaseOrgId"]);
                var supplierId = sqlReader.IsDBNull(sqlReader.GetOrdinal("VendorId")) ? 0 : Convert.ToInt32(sqlReader["VendorId"]);
                var paymentTermId = sqlReader.IsDBNull(sqlReader.GetOrdinal("PaymentTermId")) ? 0 : Convert.ToInt32(sqlReader["PaymentTermId"]);
                var incotermId = sqlReader.IsDBNull(sqlReader.GetOrdinal("IncoTermId")) ? 0 : Convert.ToInt32(sqlReader["IncoTermId"]);
                var incotermRemark = sqlReader.IsDBNull(sqlReader.GetOrdinal("IncoTermRemark")) ? null : sqlReader["IncoTermRemark"].ToString();
                var companyId = sqlReader.IsDBNull(sqlReader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(sqlReader["ClientSAPId"]);
                
                // Validate foreign keys
                bool isValid = true;
                var errors = new List<string>();
                
                if (supplierId == 0)
                {
                    errors.Add("supplier_id is NULL or 0");
                    isValid = false;
                }
                else if (!validSupplierIds.Contains(supplierId))
                {
                    errors.Add($"supplier_id {supplierId} not found in supplier_master");
                    isValid = false;
                }
                
                if (paymentTermId != 0 && !validPaymentTermIds.Contains(paymentTermId))
                {
                    errors.Add($"payment_term_id {paymentTermId} not found in payment_term_master");
                    isValid = false;
                }
                
                if (incotermId != 0 && !validIncotermIds.Contains(incotermId))
                {
                    errors.Add($"incoterm_id {incotermId} not found in incoterm_master");
                    isValid = false;
                }
                
                if (!isValid)
                {
                    if (skippedCount < 10) // Log first 10 skips
                    {
                        _logger?.LogWarning($"⚠️ Skipping VendorPOTermId {vendorPoTermId}: {string.Join(", ", errors)}");
                    }
                    skippedCount++;
                    continue;
                }
                
                // Add to batch
                batch.Add(new SupplierPaymentIncotermRecord
                {
                    SupplierPaymentIncotermId = vendorPoTermId,
                    PurchaseOrganizationId = purchaseOrgId,
                    SupplierId = supplierId,
                    PaymentTermId = paymentTermId,
                    IncotermId = incotermId,
                    IncotermRemark = incotermRemark,
                    CompanyId = companyId
                });
                
                // Insert batch when it reaches BATCH_SIZE
                if (batch.Count >= BATCH_SIZE)
                {
                    insertedCount += await InsertBatchAsync(pgConn, transaction, batch);
                    
                    _logger?.LogInformation($"📈 Progress: {totalCount:N0} processed | {insertedCount:N0} inserted | {skippedCount:N0} skipped");
                    
                    batch.Clear();
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError($"❌ Error processing record at position {totalCount}: {ex.Message}");
                throw;
            }
        }
        
        // Insert remaining batch
        if (batch.Count > 0)
        {
            insertedCount += await InsertBatchAsync(pgConn, transaction, batch);
            batch.Clear();
        }
        
        stopwatch.Stop();
        
        _logger?.LogInformation("═══════════════════════════════════════════════════════════════");
        _logger?.LogInformation("📊 Supplier Payment Incoterm Migration Summary");
        _logger?.LogInformation("═══════════════════════════════════════════════════════════════");
        _logger?.LogInformation($"  ✓ Total processed: {totalCount:N0}");
        _logger?.LogInformation($"  ✓ Successfully inserted: {insertedCount:N0}");
        _logger?.LogInformation($"  ⚠️ Skipped (invalid FKs): {skippedCount:N0}");
        _logger?.LogInformation($"  ⏱️ Duration: {stopwatch.Elapsed:hh\\:mm\\:ss}");
        _logger?.LogInformation($"  🚀 Throughput: {(totalCount / stopwatch.Elapsed.TotalSeconds):N0} records/sec");
        _logger?.LogInformation("═══════════════════════════════════════════════════════════════");
        
        return insertedCount;
    }
    
    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction, List<SupplierPaymentIncotermRecord> batch)
    {
        if (batch.Count == 0) return 0;
        
        var sql = new StringBuilder();
        sql.AppendLine(@"
            INSERT INTO supplier_payment_incoterm (
                supplier_payment_incoterm_id,
                purchase_organization_id,
                supplier_id,
                payment_term_id,
                incoterm_id,
                incoterm_remark,
                company_id,
                created_by,
                created_date,
                modified_by,
                modified_date,
                is_deleted,
                deleted_by,
                deleted_date
            ) VALUES");
        
        using var cmd = new NpgsqlCommand();
        cmd.Connection = pgConn;
        cmd.Transaction = transaction;
        
        var values = new List<string>();
        for (int i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            var paramPrefix = $"p{i}_";
            
            values.Add($@"(
                @{paramPrefix}supplier_payment_incoterm_id,
                @{paramPrefix}purchase_organization_id,
                @{paramPrefix}supplier_id,
                @{paramPrefix}payment_term_id,
                @{paramPrefix}incoterm_id,
                @{paramPrefix}incoterm_remark,
                @{paramPrefix}company_id,
                @{paramPrefix}created_by,
                @{paramPrefix}created_date,
                @{paramPrefix}modified_by,
                @{paramPrefix}modified_date,
                @{paramPrefix}is_deleted,
                @{paramPrefix}deleted_by,
                @{paramPrefix}deleted_date
            )");
            
            cmd.Parameters.AddWithValue($"@{paramPrefix}supplier_payment_incoterm_id", record.SupplierPaymentIncotermId);
            cmd.Parameters.AddWithValue($"@{paramPrefix}purchase_organization_id", record.PurchaseOrganizationId);
            cmd.Parameters.AddWithValue($"@{paramPrefix}supplier_id", record.SupplierId);
            cmd.Parameters.AddWithValue($"@{paramPrefix}payment_term_id", record.PaymentTermId);
            cmd.Parameters.AddWithValue($"@{paramPrefix}incoterm_id", record.IncotermId);
            cmd.Parameters.AddWithValue($"@{paramPrefix}incoterm_remark", (object?)record.IncotermRemark ?? DBNull.Value);
            cmd.Parameters.AddWithValue($"@{paramPrefix}company_id", record.CompanyId);
            cmd.Parameters.AddWithValue($"@{paramPrefix}created_by", 0);
            cmd.Parameters.AddWithValue($"@{paramPrefix}created_date", DateTime.UtcNow);
            cmd.Parameters.AddWithValue($"@{paramPrefix}modified_by", DBNull.Value);
            cmd.Parameters.AddWithValue($"@{paramPrefix}modified_date", DBNull.Value);
            cmd.Parameters.AddWithValue($"@{paramPrefix}is_deleted", false);
            cmd.Parameters.AddWithValue($"@{paramPrefix}deleted_by", DBNull.Value);
            cmd.Parameters.AddWithValue($"@{paramPrefix}deleted_date", DBNull.Value);
        }
        
        sql.Append(string.Join(",\n", values));
        sql.AppendLine(";");
        
        cmd.CommandText = sql.ToString();
        await cmd.ExecuteNonQueryAsync();
        
        return batch.Count;
    }
    
    private class SupplierPaymentIncotermRecord
    {
        public int SupplierPaymentIncotermId { get; set; }
        public int PurchaseOrganizationId { get; set; }
        public int SupplierId { get; set; }
        public int PaymentTermId { get; set; }
        public int IncotermId { get; set; }
        public string? IncotermRemark { get; set; }
        public int CompanyId { get; set; }
    }
}
