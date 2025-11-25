using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class ARCMainMigration : MigrationService
{
    // SQL Server: TBL_ARCMain -> PostgreSQL: arc_header
    // Using aliases that match PostgreSQL column names
    protected override string SelectQuery => @"
        SELECT 
            ARCMainId,
            ARCName,
            ARCNo,
            ARCDescription,
            SupplierName,
            VendorCode,
            VendorQuotationRef,
            FromDate,
            ToDate,
            Remarks,
            Status,
            VendorId,
            TotalARCValue,
            QuantityLimitation,
            ValueTolerance,
            QuantityTolerance,
            ClientSAPId,
            EventId,
            SummaryNote,
            ClosingNegotiationNote,
            HeaderNote,
            UsedTotalValue,
            ARCTerms,
            CreatedBy,
            CreatedDate,
            UpdatedBy,
            UpdatedDate
        FROM TBL_ARCMain";

    protected override string InsertQuery => @"
        INSERT INTO arc_header (
            arc_header_id,
            arc_name,
            arc_number,
            arc_description,
            supplier_name,
            supplier_code,
            supplier_quote_ref,
            arc_start_date,
            arc_end_date,
            remarks,
            approval_status,
            supplier_id,
            total_arc_value,
            arc_type,
            value_tolerance_percentage,
            quantity_tolerance_percentage,
            company_id,
            event_id,
            summary_note,
            closing_negotiation_note,
            header_note,
            used_total_value,
            arc_terms,
            created_by, 
            created_date, 
            modified_by, 
            modified_date, 
            is_deleted, 
            deleted_by, 
            deleted_date
        ) VALUES (
            @arc_header_id,
            @arc_name,
            @arc_number,
            @arc_description,
            @supplier_name,
            @supplier_code,
            @supplier_quote_ref,
            @arc_start_date,
            @arc_end_date,
            @remarks,
            @approval_status,
            @supplier_id,
            @total_arc_value,
            @arc_type,
            @value_tolerance_percentage,
            @quantity_tolerance_percentage,
            @company_id,
            @event_id,
            @summary_note,
            @closing_negotiation_note,
            @header_note,
            @used_total_value,
            @arc_terms,
            @created_by, 
            @created_date, 
            @modified_by, 
            @modified_date, 
            @is_deleted, 
            @deleted_by, 
            @deleted_date
        )";

    public ARCMainMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // arc_header_id
            "Direct", // arc_name
            "Direct", // arc_number
            "Direct", // arc_description
            "Direct", // supplier_name
            "Direct", // supplier_code
            "Direct", // supplier_quote_ref
            "Direct", // arc_start_date
            "Direct", // arc_end_date
            "Direct", // remarks
            "Direct", // approval_status
            "Direct", // supplier_id
            "Direct", // total_arc_value
            "Direct", // arc_type
            "Direct", // value_tolerance_percentage
            "Direct", // quantity_tolerance_percentage
            "Direct", // company_id
            "Direct", // event_id
            "Direct", // summary_note
            "Direct", // closing_negotiation_note
            "Direct", // header_note
            "Direct", // used_total_value
            "Direct"  // arc_terms
        };
    }

    public async Task<int> MigrateAsync()
    {
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);

        int insertedCount = 0;
        int skippedCount = 0;

        while (await reader.ReadAsync())
        {
            try
            {
                pgCmd.Parameters.Clear();

                pgCmd.Parameters.AddWithValue("@arc_header_id", reader["ARCMainId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_name", reader["ARCName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_number", reader["ARCNo"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_description", reader["ARCDescription"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_name", reader["SupplierName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_code", reader["VendorCode"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_quote_ref", reader["VendorQuotationRef"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_start_date", reader["FromDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_end_date", reader["ToDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@remarks", reader["Remarks"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@approval_status", reader["Status"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@supplier_id", reader["VendorId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@total_arc_value", reader["TotalARCValue"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_type", reader["QuantityLimitation"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@value_tolerance_percentage", reader["ValueTolerance"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@quantity_tolerance_percentage", reader["QuantityTolerance"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@event_id", reader["EventId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@summary_note", reader["SummaryNote"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@closing_negotiation_note", reader["ClosingNegotiationNote"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@header_note", reader["HeaderNote"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@used_total_value", reader["UsedTotalValue"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@arc_terms", reader["ARCTerms"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_by", reader["CreatedBy"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_date", reader["CreatedDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_by", reader["UpdatedBy"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_date", reader["UpdatedDate"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@is_deleted", false);
                pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                int result = await pgCmd.ExecuteNonQueryAsync();
                if (result > 0) insertedCount++;
            }
            catch (Exception ex)
            {
                skippedCount++;
                Console.WriteLine($"❌ Error migrating ARCMainId {reader["ARCMainId"]}: {ex.Message}");
                Console.WriteLine($"   Stack Trace: {ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner Exception: {ex.InnerException.Message}");
                }
                // Continue with next record
            }
        }

        if (skippedCount > 0)
        {
            Console.WriteLine($"Warning: {skippedCount} records were skipped due to errors.");
        }

        return insertedCount;
    }
}
