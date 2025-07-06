import sqlite3
import pandas as pd
import logging

# Setup logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Function to create vendor sales summary using SQL query
def create_vendor_summary(conn):
    """
    This function merges different tables to get the overall vendor summary.
    """
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """, conn)

    return vendor_sales_summary

# Function to clean and enrich data
def clean_data(df):
    """
    Cleans the data and adds derived metrics.
    """
    # Convert Volume to float
    df['Volume'] = df['Volume'].astype(float)

    # Fill missing values
    df.fillna(0, inplace=True)

    # Strip spaces from text fields
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    #creating new column for new for better analysis
    vendor_sales_summary['GrossProfit']=vendor_sales_summary['TotalSalesDollars'] vendor_sales_summary ['TotalPurchaseDollars']
    vendor_sales_summary['profitMargin']=(vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnover']= vendor_sales_summary ['TotalSalesQuantity'] / vendor_sales_summary ['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio']= vendor_sales_summary   ['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

    return df

# Ingesting cleaned data to SQLite DB
def ingest_db(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='replace', index=False)

# Main execution
if __name__ == '__main__':
    try:
        conn = sqlite3.connect('inventory.db')
        logging.info('Creating Vendor Summary Table.....')
        summary_df = create_vendor_summary(conn)

        logging.info('Cleaning Data.....')
        clean_df = clean_data(summary_df)

        logging.info('Ingesting Data.....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)

        logging.info('Completed Successfully.')

    except Exception as e:
        logging.error(f'Error: {e}')
    finally:
        conn.close()
