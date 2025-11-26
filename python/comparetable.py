import pandas as pd
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from db_config import (
    get_mssql_connection, 
    get_postgres_connection,
    print_header,
    get_output_path
)

print_header("Simple Table Comparison - Side by Side")

# Connect to databases
print("\n[1/3] Connecting to MSSQL...")
mssql_conn = get_mssql_connection()
print("✓ MSSQL Connected")

print("\n[2/3] Connecting to PostgreSQL...")
pg_conn = get_postgres_connection()
print("✓ PostgreSQL Connected")

# Get table names from user
print("\n" + "=" * 80)
mssql_table = input("Enter MSSQL table name: ")
pg_table = input("Enter PostgreSQL table name: ")
print("=" * 80)

print(f"\nComparing:")
print(f"  MSSQL: {mssql_table}")
print(f"  PostgreSQL: {pg_table}")

try:
    # Get MSSQL columns
    print("\n[3/3] Fetching table structures...")
    
    # Use direct cursor approach for better control
    cursor = mssql_conn.cursor()
    cursor.execute("""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """, (mssql_table,))
    
    mssql_rows = cursor.fetchall()
    cursor.close()
    
    if len(mssql_rows) == 0:
        print(f"✗ Table '{mssql_table}' not found in MSSQL!")
        mssql_conn.close()
        pg_conn.close()
        input("\nPress Enter to exit...")
        exit()
    
    # Convert to DataFrame - handle the cursor result properly
    mssql_data = []
    for row in mssql_rows:
        mssql_data.append({'COLUMN_NAME': row[0], 'DATA_TYPE': row[1]})
    df_mssql = pd.DataFrame(mssql_data)
    
    print(f"✓ MSSQL: {len(df_mssql)} columns")
    
    # Get PostgreSQL columns using cursor approach
    cursor = pg_conn.cursor()
    cursor.execute("""
        SELECT 
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (pg_table,))
    
    pg_rows = cursor.fetchall()
    cursor.close()
    
    if len(pg_rows) == 0:
        print(f"✗ Table '{pg_table}' not found in PostgreSQL!")
        mssql_conn.close()
        pg_conn.close()
        input("\nPress Enter to exit...")
        exit()
    
    # Convert to DataFrame - handle the cursor result properly
    pg_data = []
    for row in pg_rows:
        pg_data.append({'column_name': row[0], 'data_type': row[1]})
    df_pg = pd.DataFrame(pg_data)
    
    print(f"✓ PostgreSQL: {len(df_pg)} columns")
    
    # Create side-by-side comparison data
    max_rows = max(len(df_mssql), len(df_pg))
    
    comparison_data = []
    
    for i in range(max_rows):
        row_data = {}
        
        # MSSQL columns
        if i < len(df_mssql):
            row_data['MSSQL_COLUMN_NAME'] = df_mssql.iloc[i]['COLUMN_NAME']
            row_data['MSSQL_DATA_TYPE'] = df_mssql.iloc[i]['DATA_TYPE']
        else:
            row_data['MSSQL_COLUMN_NAME'] = ''
            row_data['MSSQL_DATA_TYPE'] = ''
        
        # Empty separator column
        row_data['SEPARATOR'] = ''
        
        # PostgreSQL columns
        if i < len(df_pg):
            row_data['PG_column_name'] = df_pg.iloc[i]['column_name']
            row_data['PG_data_type'] = df_pg.iloc[i]['data_type']
        else:
            row_data['PG_column_name'] = ''
            row_data['PG_data_type'] = ''
        
        comparison_data.append(row_data)
    
    df_comparison = pd.DataFrame(comparison_data)
    
    # Save to Excel
    output_file = get_output_path(f"Compare_{mssql_table}_vs_{pg_table}.xlsx")
    
    # Write to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_comparison.to_excel(writer, sheet_name='Comparison', index=False, startrow=1)
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['Comparison']
        
        # Add table names in first row
        worksheet['A1'] = f'mssql - {mssql_table}'
        worksheet['D1'] = f'postgres - {pg_table}'
        
        # Merge cells for headers
        worksheet.merge_cells('A1:B1')
        worksheet.merge_cells('D1:E1')
        
        # Style the header row (table names)
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF", size=12)
        center_align = Alignment(horizontal='center', vertical='center')
        
        for cell in ['A1', 'D1']:
            worksheet[cell].fill = header_fill
            worksheet[cell].font = header_font
            worksheet[cell].alignment = center_align
        
        # Style column headers (COLUMN_NAME, DATA_TYPE, etc.)
        col_header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
        col_header_font = Font(bold=True, size=11)
        
        # Rename column headers
        worksheet['A2'] = 'COLUMN_NAME'
        worksheet['B2'] = 'DATA_TYPE'
        worksheet['C2'] = ''
        worksheet['D2'] = 'column_name'
        worksheet['E2'] = 'data_type'
        
        for cell in ['A2', 'B2', 'D2', 'E2']:
            worksheet[cell].fill = col_header_fill
            worksheet[cell].font = col_header_font
            worksheet[cell].alignment = center_align
        
        # Set column widths
        worksheet.column_dimensions['A'].width = 20
        worksheet.column_dimensions['B'].width = 18
        worksheet.column_dimensions['C'].width = 3
        worksheet.column_dimensions['D'].width = 25
        worksheet.column_dimensions['E'].width = 25
        
        # Add borders
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row in worksheet.iter_rows(min_row=1, max_row=max_rows+2, min_col=1, max_col=5):
            for cell in row:
                if cell.column != 3:  # Skip separator column
                    cell.border = thin_border
                    cell.alignment = Alignment(vertical='center')
        
        # Make separator column C gray
        gray_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
        for row in range(1, max_rows + 3):
            worksheet[f'C{row}'].fill = gray_fill
    
    print("\n" + "=" * 80)
    print("✓ COMPARISON COMPLETE!")
    print("=" * 80)
    print(f"\n✓ Simple side-by-side comparison saved to:")
    print(f"  {output_file}")
    print(f"\nFormat:")
    print(f"  - MSSQL columns on the left")
    print(f"  - PostgreSQL columns on the right")
    print(f"  - Clean, formatted Excel sheet")

except Exception as e:
    print(f"\n✗ Comparison Failed!")
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    mssql_conn.close()
    pg_conn.close()
    print("\n" + "=" * 80)
    print("Connections closed")
    print("=" * 80)

input("\nPress Enter to exit...")




