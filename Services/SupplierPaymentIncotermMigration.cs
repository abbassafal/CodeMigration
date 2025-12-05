using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Threading;
using DataMigration.Services;

public class SupplierPaymentIncotermMigration : MigrationService
{
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
        ORDER BY VendorPayIncoTermId";

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

    public SupplierPaymentIncotermMigration(IConfiguration configuration) : base(configuration) { }

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
        // Example implementation: migrate all records from MSSQL to Postgres
        int insertedCount = 0;
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            using var insertCmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);
            insertCmd.Parameters.AddWithValue("@supplier_payment_incoterm_id", reader["VendorPOTermId"]);
            insertCmd.Parameters.AddWithValue("@purchase_organization_id", reader["PurchaseOrgId"]);
            insertCmd.Parameters.AddWithValue("@supplier_id", reader["VendorId"]);
            insertCmd.Parameters.AddWithValue("@payment_term_id", reader["PaymentTermId"]);
            insertCmd.Parameters.AddWithValue("@incoterm_id", reader["IncoTermId"]);
            insertCmd.Parameters.AddWithValue("@incoterm_remark", reader["IncoTermRemark"] ?? (object)DBNull.Value);
            insertCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            insertCmd.Parameters.AddWithValue("@created_by", 0);
            insertCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            insertCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
            insertCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
            insertCmd.Parameters.AddWithValue("@is_deleted", false);
            insertCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
            insertCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
            insertedCount += await insertCmd.ExecuteNonQueryAsync();
        }
        return insertedCount;
    }
}
