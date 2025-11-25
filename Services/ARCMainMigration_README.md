# ARC Main Migration - Field Mapping Documentation

## Overview
This document explains the field mapping between SQL Server `TBL_ARCMain` table and PostgreSQL `arc_header` table.

## Source and Target Tables
- **Source (SQL Server):** `TBL_ARCMain`
- **Target (PostgreSQL):** `arc_header`
- **Migration Service:** `ARCMainMigration.cs`

## Field Mapping Details

| SQL Server Column | PostgreSQL Column | Data Type (SQL) | Data Type (PG) | Mapping Logic | Notes |
|-------------------|-------------------|-----------------|----------------|---------------|-------|
| `ARCMainId` | `arc_header_id` | int (PK) | integer (PK) | Direct | Primary key, auto-increment |
| `ARCName` | `arc_name` | nvarchar | varchar | Direct | ARC name |
| `ARCNo` | `arc_number` | nvarchar(200) | varchar | Direct | ARC number/reference |
| `ARCDescription` | `arc_description` | nvarchar | text | Direct | Description |
| `SupplierName` | `supplier_name` | nvarchar | varchar | Direct | Supplier name |
| `VendorCode` | `supplier_code` | nchar(20) | varchar | Direct | Supplier/Vendor code |
| `VendorQuotationRef` | `supplier_quote_ref` | nvarchar | varchar | Direct | Quote reference |
| `FromDate` | `arc_start_date` | datetime | timestamp | Direct | ARC start date |
| `ToDate` | `arc_end_date` | datetime | timestamp | Direct | ARC end date |
| `Remarks` | `remarks` | nvarchar | text | Direct | General remarks |
| `CreatedBy` | `created_by` | int | integer | Direct | Created by user ID |
| `CreatedDate` | `created_date` | datetime | timestamp | Direct | Creation timestamp |
| `Status` | `approval_status` | nvarchar | varchar | Direct | Approval status |
| `UpdatedBy` | `modified_by` | int | integer | Direct | Last modified by user ID |
| `UpdatedDate` | `modified_date` | datetime | timestamp | Direct | Last modification timestamp |
| `VendorId` | `supplier_id` | int | integer (FK) | Direct | Foreign key to supplier table |
| `ApprovedDate` | `approved_date` | datetime | timestamp | Direct | Approval date |
| `ExtendToDate` | `extend_to_date` | datetime | timestamp | Direct | Extended date |
| `RecallDate` | `recall_date` | datetime | timestamp | Direct | Recall date |
| `TotalARCValue` | `total_arc_value` | decimal | numeric | Direct | Total ARC value |
| `QuantityLimitation` | `arc_type` | int | integer | Direct | ARC type/quantity limitation |
| `ValueTolerance` | `value_tolerance_percentage` | decimal | numeric | Direct | Value tolerance % |
| `QuantityTolerance` | `quantity_tolerance_percentage` | decimal | numeric | Direct | Quantity tolerance % |
| `ShipTo` | `ship_to` | int | integer | Direct | Ship to location |
| `ClientSAPId` | `company_id` | int | integer (FK) | Direct | Company/Client ID |
| `PaymentTermId` | `payment_term_id` | int | integer (FK) | Direct | Payment term FK |
| `IncoTermId` | `inco_term_id` | int | integer (FK) | Direct | Incoterm FK |
| `EventId` | `event_id` | int | integer (FK) | Direct | Event FK |
| `SummaryNote` | `summary_note` | varchar(4000) | text | Direct | Summary notes |
| `ClosingNegotiationNote` | `closing_negotiation_note` | varchar(4000) | text | Direct | Closing negotiation notes |
| `HeaderNote` | `header_note` | varchar(4000) | text | Direct | Header notes |
| `UsedTotalValue` | `used_total_value` | decimal | numeric | Direct | Used total value |
| `ARCTerms` | `arc_terms` | nvarchar | text | Direct | ARC terms and conditions |
| `OldToDate` | `old_to_date` | datetime | timestamp | Direct | Previous end date |
| - | `is_deleted` | - | boolean | Default: false | Soft delete flag |
| - | `deleted_by` | - | integer | Default: null | Deleted by user ID |
| - | `deleted_date` | - | timestamp | Default: null | Deletion timestamp |

## Key Features

### 1. **Direct Field Mapping**
Most fields have a 1:1 direct mapping from SQL Server to PostgreSQL with appropriate data type conversions.

### 2. **Foreign Key Handling**
The migration handles foreign keys with null/zero value checking:
- `VendorId` → `supplier_id`
- `ClientSAPId` → `company_id`
- `PaymentTermId` → `payment_term_id`
- `IncoTermId` → `inco_term_id`
- `EventId` → `event_id`
- `ShipTo` → `ship_to`

If a foreign key value is `0` or `NULL`, it's converted to `DBNull.Value` to avoid FK constraint violations.

### 3. **Date/Time Handling**
All `datetime` fields from SQL Server are mapped to `timestamp` in PostgreSQL:
- `FromDate` → `arc_start_date`
- `ToDate` → `arc_end_date`
- `CreatedDate` → `created_date`
- `UpdatedDate` → `modified_date`
- `ApprovedDate`, `ExtendToDate`, `RecallDate`, `OldToDate`

### 4. **Audit Fields**
Standard audit fields are added for PostgreSQL:
- `is_deleted` = `false` (default)
- `deleted_by` = `null`
- `deleted_date` = `null`

### 5. **Error Handling**
The migration includes try-catch error handling to:
- Continue processing if a single record fails
- Log the error with the `ARCMainId`
- Count and report skipped records

## Usage

### Via Web Interface
1. Navigate to the Migration page
2. Select **"arcmain"** from the table dropdown
3. View field mappings (optional)
4. Click **"Migrate"** button

### Via API
```http
POST /Migration/MigrateAsync
Content-Type: application/json

{
  "table": "arcmain"
}
```

### View Mappings
```http
GET /Migration/GetMappings?table=arcmain
```

## Dependencies

### Required Master Data (must be migrated first):
1. ✅ **Supplier/Vendor Master** - for `supplier_id` FK
2. ✅ **Company Master** - for `company_id` FK
3. ✅ **Payment Term Master** - for `payment_term_id` FK
4. ✅ **Incoterm Master** - for `inco_term_id` FK
5. ✅ **Event Master** - for `event_id` FK

### Optional Dependencies:
- User Master - for `created_by`, `modified_by`

## Migration Order Recommendation

```
1. Currency Master
2. UOM Master
3. Plant Master
4. Material Group Master
5. Purchase Group Master
6. Payment Term Master
7. Incoterm Master (if exists)
8. Event Master
9. Tax Master
10. Users Master
11. Supplier/Vendor Master (if exists)
12. Company Master
13. → ARC Main (TBL_ARCMain → arc_header)
```

## Data Validation

### Before Migration:
- Verify all foreign key references exist in target database
- Check for null values in required fields
- Validate date ranges (FromDate < ToDate)
- Ensure primary key values are unique

### After Migration:
- Compare record counts: `SELECT COUNT(*) FROM TBL_ARCMain` vs `SELECT COUNT(*) FROM arc_header`
- Verify foreign key integrity
- Check date field conversions
- Validate decimal/numeric precision

## SQL Validation Queries

### Record Count Comparison
```sql
-- SQL Server
SELECT COUNT(*) as total_records FROM TBL_ARCMain;

-- PostgreSQL
SELECT COUNT(*) as total_records FROM arc_header;
```

### Check Foreign Key References
```sql
-- PostgreSQL - Check for missing FK references
SELECT arc_header_id, supplier_id 
FROM arc_header 
WHERE supplier_id IS NOT NULL 
  AND supplier_id NOT IN (SELECT supplier_id FROM suppliers);
```

### Validate Data Ranges
```sql
-- PostgreSQL - Check date ranges
SELECT arc_header_id, arc_start_date, arc_end_date
FROM arc_header
WHERE arc_start_date > arc_end_date;
```

## Common Issues and Solutions

### Issue 1: Foreign Key Constraint Violations
**Symptom:** Migration fails with FK constraint error
**Solution:** 
- Migrate dependent master tables first
- Run FK validation queries before migration
- Check for `0` or invalid FK values in source data

### Issue 2: Date Conversion Issues
**Symptom:** Invalid date values after migration
**Solution:**
- SQL Server `datetime` has different range than PostgreSQL `timestamp`
- Check for dates before 1970-01-01 or after 2038-01-19
- Handle null dates properly

### Issue 3: Decimal Precision Loss
**Symptom:** Values differ slightly after migration
**Solution:**
- Verify decimal scale in both databases
- Use appropriate precision for monetary values
- Consider rounding strategy

## Performance Considerations

- **Batch Size:** Processes one record at a time (can be optimized for bulk insert)
- **Indexes:** Ensure indexes exist on FK columns in target table
- **Constraints:** Disable constraints during migration for better performance (re-enable after)
- **Estimated Time:** ~100-1000 records per second (depends on data and network)

## Monitoring

Monitor the migration progress:
```csharp
// The service returns:
// - insertedCount: Successfully migrated records
// - skippedCount: Records that failed (logged to console)
```

## Rollback Strategy

If migration needs to be rolled back:
```sql
-- PostgreSQL - Delete migrated records
DELETE FROM arc_header WHERE created_date >= 'MIGRATION_START_TIMESTAMP';

-- Or truncate if no other data exists
TRUNCATE TABLE arc_header CASCADE;
```

## Testing Checklist

- [ ] Test migration with sample dataset (5-10 records)
- [ ] Verify all field mappings are correct
- [ ] Check foreign key references
- [ ] Validate date conversions
- [ ] Test with null values
- [ ] Test with maximum length values
- [ ] Verify numeric precision
- [ ] Check error handling with invalid data
- [ ] Compare record counts
- [ ] Verify data integrity after migration

## Contact

For issues or questions about the ARC Main migration, refer to:
- Service file: `Services/ARCMainMigration.cs`
- Controller: `Controller/MigrationController.cs`
- Configuration: `appsettings.json`
