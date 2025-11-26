using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class MaterialMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT ITEMID, ITEMCODE, ITEMNAME, ITEMDESCRIPTION, UOMId, MaterialGroupId, ClientSAPId FROM TBL_ITEMMASTER";
    protected override string InsertQuery => @"INSERT INTO material_master (material_id, material_code, material_name, material_description, uom_id, material_group_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@material_id, @material_code, @material_name, @material_description, @uom_id, @material_group_id, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public MaterialMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "ITEMID -> material_id (Direct)",
            "ITEMCODE -> material_code (Direct)",
            "ITEMNAME -> material_name (Direct)",
            "ITEMDESCRIPTION -> material_description (Direct)",
            "UOMId -> uom_id (FK to uom_master)",
            "MaterialGroupId -> material_group_id (FK to material_group_master)",
            "ClientSAPId -> company_id (FK to company)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
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

        int insertedCount = 0;
        int processedCount = 0;
        
        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                try
                {
                    // Validate field values before processing
                    var itemId = reader.IsDBNull(reader.GetOrdinal("ITEMID")) ? 0 : Convert.ToInt32(reader["ITEMID"]);
                    var itemCode = reader.IsDBNull(reader.GetOrdinal("ITEMCODE")) ? "" : reader["ITEMCODE"].ToString();
                    var itemName = reader.IsDBNull(reader.GetOrdinal("ITEMNAME")) ? "" : reader["ITEMNAME"].ToString();
                    var itemDescription = reader.IsDBNull(reader.GetOrdinal("ITEMDESCRIPTION")) ? "" : reader["ITEMDESCRIPTION"].ToString();
                    var uomId = reader.IsDBNull(reader.GetOrdinal("UOMId")) ? 0 : Convert.ToInt32(reader["UOMId"]);
                    var materialGroupId = reader.IsDBNull(reader.GetOrdinal("MaterialGroupId")) ? 0 : Convert.ToInt32(reader["MaterialGroupId"]);
                    var clientSapId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(reader["ClientSAPId"]);

                    // Skip records where any required FK is 0 to avoid constraint violations
                    if (uomId == 0 || materialGroupId == 0)
                    {
                        Console.WriteLine($"Skipping record {processedCount} (ITEMID: {itemId}) - Missing required FK: UOMId={uomId}, MaterialGroupId={materialGroupId}");
                        continue;
                    }

                    // Validate required fields
                    if (string.IsNullOrWhiteSpace(itemCode))
                    {
                        throw new Exception($"Invalid ITEMCODE at record {processedCount} (ITEMID: {itemId}) - Cannot be null or empty");
                    }

                    pgCmd.Parameters.Clear();
                    pgCmd.Parameters.AddWithValue("@material_id", itemId);
                    pgCmd.Parameters.AddWithValue("@material_code", itemCode ?? "");
                    pgCmd.Parameters.AddWithValue("@material_name", itemName ?? "");
                    pgCmd.Parameters.AddWithValue("@material_description", itemDescription ?? "");
                    pgCmd.Parameters.AddWithValue("@uom_id", uomId);
                    pgCmd.Parameters.AddWithValue("@material_group_id", materialGroupId);
                    pgCmd.Parameters.AddWithValue("@company_id", clientSapId);
                    pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
                    pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
                    pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
                    pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
                    pgCmd.Parameters.AddWithValue("@is_deleted", false); // Default: false
                    pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
                    pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null
                    
                    int result = await pgCmd.ExecuteNonQueryAsync();
                    if (result > 0) insertedCount++;
                }
                catch (Exception recordEx)
                {
                    throw new Exception($"Error processing record {processedCount} in Material Master Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception readerEx)
        {
            // Check if it's a connection or stream issue
            if (readerEx.Message.Contains("reading from stream") || readerEx.Message.Contains("connection") || readerEx.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during Material Master Migration after processing {processedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {readerEx.Message}", readerEx);
            }
            else if (readerEx.Message.Contains("constraint") || readerEx.Message.Contains("foreign key") || readerEx.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during Material Master Migration at record {processedCount}. " +
                                  $"This could be due to: 1) Missing reference data in UOM or MaterialGroup tables, " +
                                  $"2) Duplicate primary keys, 3) Invalid foreign key values. Original error: {readerEx.Message}", readerEx);
            }
            else
            {
                throw new Exception($"Unexpected error during Material Master Migration at record {processedCount}: {readerEx.Message}", readerEx);
            }
        }
        
        return insertedCount;
    }
}