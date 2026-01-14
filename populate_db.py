### Utility Functions
import pandas as pd
import os
import psycopg2
from psycopg2 import extras
import csv
from pathlib import Path
import time
import datetime

from utils import get_db_url


def create_connection(db_url):
    """Create a database connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(e)
        return None


def create_table(conn, create_table_sql, drop_table_name=None):
    """Create a table in PostgreSQL database"""
    with conn.cursor() as c:
        if drop_table_name:
            try:
                c.execute(f"DROP TABLE IF EXISTS {drop_table_name} CASCADE")
            except Exception as e:
                print(e)

        try:
            c.execute(create_table_sql)
        except Exception as e:
            print(e)
    # No commit - caller handles transaction


def execute_sql_statement(sql_statement, conn):
    """Execute SQL statement and return results"""
    with conn.cursor() as cur:
        cur.execute(sql_statement)
        return cur.fetchall()


def drop_all_tables(db_url):
    """Drop all tables in the correct order (inverse of creation order)"""
    conn = create_connection(db_url)
    if conn is None:
        return
    
    tables_to_drop = [
        'OrderDetail',
        'Product',
        'ProductCategory', 
        'Customer',
        'Country',
        'Region'
    ]
    
    with conn:
        with conn.cursor() as cur:
            for table in tables_to_drop:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    print(f"Dropped table: {table}")
                except Exception as e:
                    print(f"Error dropping table {table}: {e}")
    
    conn.close()


def create_all_tables(data_filename, db_url):
    """Create all tables in the correct order in a single function"""
    
    # Step 1: Create Region table
    print("Step 1: Creating Region table...")
    regions = set()
    with open(data_filename, 'r', encoding='utf-8') as file:
        headers = file.readline().strip().split('\t')
        region_index = headers.index('Region')

        for line in file:
            if line.strip():
                columns = line.strip().split('\t')
                regions.add(columns[region_index])

    sorted_regions = sorted(regions)

    conn = create_connection(db_url)
    with conn:
        create_region_table_statement = """
        CREATE TABLE Region (
            RegionID SERIAL PRIMARY KEY,
            Region TEXT NOT NULL
        )
        """
        create_table(conn, create_region_table_statement, "Region")

        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO Region (Region) VALUES (%s)",
                [(r,) for r in sorted_regions]
            )
    
    # Step 2: Get Region to RegionID dictionary
    print("Step 2: Creating Region dictionary...")
    region_dict = {}
    conn = create_connection(db_url)
    with conn:
        rows = execute_sql_statement(
            "SELECT RegionID, Region FROM Region", conn
        )
        for region_id, region_name in rows:
            region_dict[region_name] = region_id
    
    # Step 3: Create Country table
    print("Step 3: Creating Country table...")
    country_region_pairings = set()
    with open(data_filename, 'r', encoding='utf-8') as file:
        headers = file.readline().strip().split('\t')
        country_index = headers.index('Country')
        region_index = headers.index('Region')

        for line in file:
            if line.strip():
                columns = line.strip().split('\t')
                country_region_pairings.add(
                    (columns[country_index], columns[region_index])
                )

    sorted_countries = sorted(country_region_pairings)

    conn = create_connection(db_url)
    with conn:
        create_country_table_statement = """
        CREATE TABLE Country (
            CountryID SERIAL PRIMARY KEY,
            Country TEXT NOT NULL,
            RegionID INTEGER NOT NULL REFERENCES Region(RegionID)
        )
        """
        create_table(conn, create_country_table_statement, "Country")

        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO Country (Country, RegionID) VALUES (%s, %s)",
                [(c, region_dict[r]) for c, r in sorted_countries]
            )
    
    # Step 4: Get Country to CountryID dictionary
    print("Step 4: Creating Country dictionary...")
    country_dict = {}
    conn = create_connection(db_url)
    with conn:
        rows = execute_sql_statement(
            "SELECT CountryID, Country FROM Country", conn
        )
        for cid, cname in rows:
            country_dict[cname] = cid
    
    # Step 5: Create Customer table
    print("Step 5: Creating Customer table...")
    customer_info = set()
    with open(data_filename, 'r', encoding='utf-8') as file:
        headers = file.readline().strip().split('\t')
        name_index = headers.index('Name')
        address_index = headers.index('Address')
        city_index = headers.index('City')
        country_index = headers.index('Country')

        for line in file:
            if line.strip():
                columns = line.strip().split('\t')
                customer_info.add((
                    columns[name_index],
                    columns[address_index],
                    columns[city_index],
                    columns[country_index]
                ))

    customer_list = sorted(customer_info)

    conn = create_connection(db_url)
    with conn:
        create_customer_table_statement = """
        CREATE TABLE Customer (
            CustomerID SERIAL PRIMARY KEY,
            FirstName TEXT NOT NULL,
            LastName TEXT NOT NULL,
            Address TEXT NOT NULL,
            City TEXT NOT NULL,
            CountryID INTEGER NOT NULL REFERENCES Country(CountryID)
        )
        """
        create_table(conn, create_customer_table_statement, "Customer")

        with conn.cursor() as cur:
            for full_name, address, city, country in customer_list:
                parts = full_name.split()
                cur.execute(
                    """
                    INSERT INTO Customer
                    (FirstName, LastName, Address, City, CountryID)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        parts[0],
                        " ".join(parts[1:]) if len(parts) > 1 else "",
                        address,
                        city,
                        country_dict[country]
                    )
                )
    
    # Step 6: Get Customer to CustomerID dictionary
    print("Step 6: Creating Customer dictionary...")
    customer_dict = {}
    conn = create_connection(db_url)
    with conn:
        rows = execute_sql_statement(
            "SELECT CustomerID, FirstName, LastName FROM Customer", conn
        )
        for cid, fn, ln in rows:
            customer_dict[f"{fn} {ln}".strip()] = cid
    
    # Step 7: Create ProductCategory table
    print("Step 7: Creating ProductCategory table...")
    categories = set()
    category_descriptions = {}
    with open(data_filename, 'r', encoding='utf-8') as file:
        headers = file.readline().strip().split('\t')
        category_index = headers.index('ProductCategory')
        description_index = headers.index('ProductCategoryDescription')

        for line in file:
            if line.strip():
                columns = line.strip().split('\t')
                category_list = columns[category_index].split(';')
                description_list = columns[description_index].split(';')

                for i, category in enumerate(category_list):
                    category = category.strip()
                    description = description_list[i].strip() if i < len(description_list) else ''
                    categories.add(category)
                    category_descriptions.setdefault(category, description)

    sorted_categories = sorted(categories)

    conn = create_connection(db_url)
    with conn:
        create_table(conn, """
        CREATE TABLE ProductCategory (
            ProductCategoryID SERIAL PRIMARY KEY,
            ProductCategory TEXT NOT NULL,
            ProductCategoryDescription TEXT NOT NULL
        )
        """, "ProductCategory")

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ProductCategory
                (ProductCategory, ProductCategoryDescription)
                VALUES (%s, %s)
                """,
                [(c, category_descriptions[c]) for c in sorted_categories]
            )
    
    # Step 8: Get ProductCategory to ProductCategoryID dictionary
    print("Step 8: Creating ProductCategory dictionary...")
    category_dict = {}
    conn = create_connection(db_url)
    with conn:
        rows = execute_sql_statement(
            "SELECT ProductCategoryID, ProductCategory FROM ProductCategory",
            conn
        )
        for cid, cname in rows:
            category_dict[cname] = cid
    
    # Step 9: Create Product table (using sets for alphabetical order)
    print("Step 9: Creating Product table...")
    products = set()
    product_categories = {}
    product_prices = {}

    with open(data_filename, 'r', encoding='utf-8') as file:
        headers = file.readline().strip().split('\t')
        product_name_index = headers.index('ProductName')
        product_category_index = headers.index('ProductCategory')
        product_unit_price_index = headers.index('ProductUnitPrice')

        for line in file:
            if line.strip():
                cols = line.strip().split('\t')
                names = cols[product_name_index].split(';')
                categories = cols[product_category_index].split(';')
                prices = cols[product_unit_price_index].split(';')

                for i, name in enumerate(names):
                    if i < len(categories) and i < len(prices):
                        product_name_clean = name.strip()
                        category_name = categories[i].strip()
                        unit_price = prices[i].strip()

                        products.add(product_name_clean)
                        if product_name_clean not in product_categories:
                            product_categories[product_name_clean] = category_name
                            product_prices[product_name_clean] = unit_price

    sorted_products = sorted(products)

    conn = create_connection(db_url)
    with conn:
        create_table(conn, """
        CREATE TABLE Product (
            ProductID SERIAL PRIMARY KEY,
            ProductName TEXT NOT NULL,
            ProductUnitPrice REAL NOT NULL,
            ProductCategoryID INTEGER NOT NULL REFERENCES ProductCategory(ProductCategoryID)
        )
        """, "Product")

        with conn.cursor() as cur:
            product_data = []
            for product_name in sorted_products:
                category_name = product_categories.get(product_name, '')
                unit_price_str = product_prices.get(product_name, '0')
                try:
                    unit_price_float = float(unit_price_str) if unit_price_str else 0.0
                except ValueError:
                    unit_price_float = 0.0
                category_id = category_dict.get(category_name, 1)
                
                product_data.append((product_name, unit_price_float, category_id))
            
            cur.executemany(
                """
                INSERT INTO Product
                (ProductName, ProductUnitPrice, ProductCategoryID)
                VALUES (%s, %s, %s)
                """,
                product_data
            )
    
    # Step 10: Get Product to ProductID dictionary
    print("Step 10: Creating Product dictionary...")
    product_dict = {}
    conn = create_connection(db_url)
    with conn:
        rows = execute_sql_statement(
            "SELECT ProductID, ProductName FROM Product",
            conn
        )
        for pid, pname in rows:
            product_dict[pname] = pid
    
    # Step 11: Create OrderDetail table
    print("Step 11: Creating OrderDetail table...")
    order_rows = []
    with open(data_filename, 'r', encoding='utf-8') as file:
        headers = file.readline().strip().split('\t')
        name_index = headers.index('Name')
        product_name_index = headers.index('ProductName')
        quantity_ordered_index = headers.index('QuantityOrderded')
        order_date_index = headers.index('OrderDate')

        line_count = 0
        for line in file:
            if line.strip():
                cols = line.strip().split('\t')
                
                # Check if we have enough columns
                if len(cols) <= max(name_index, product_name_index, quantity_ordered_index, order_date_index):
                    continue

                customer_id = customer_dict.get(cols[name_index])
                if not customer_id:
                    continue

                products_list = cols[product_name_index].split(';')
                quantities = cols[quantity_ordered_index].split(';')
                dates = cols[order_date_index].split(';')

                for i, pname in enumerate(products_list):
                    # IMPORTANT: Add bounds checking like the SQLite version
                    if i < len(quantities) and i < len(dates):
                        try:
                            qty = int(quantities[i].strip()) if quantities[i].strip() else 1
                        except (ValueError, IndexError):
                            qty = 1

                        try:
                            odate = datetime.datetime.strptime(
                                dates[i].strip(), '%Y%m%d'
                            )
                        except (ValueError, IndexError):
                            odate = datetime.datetime(1900, 1, 1)

                        pid = product_dict.get(pname.strip())
                        if pid:
                            order_rows.append((customer_id, pid, odate, qty))
            
            line_count += 1
            if line_count % 10000 == 0:
                print(f"Processed {line_count} lines, collected {len(order_rows)} order records...")

    print(f"Total: Found {len(order_rows)} order records to insert...")

    conn = create_connection(db_url)
    with conn:
        create_table(conn, """
        CREATE TABLE OrderDetail (
            OrderID SERIAL PRIMARY KEY,
            CustomerID INTEGER NOT NULL REFERENCES Customer(CustomerID),
            ProductID INTEGER NOT NULL REFERENCES Product(ProductID),
            OrderDate TIMESTAMP NOT NULL,
            QuantityOrdered INTEGER NOT NULL
        )
        """, "OrderDetail")

        print(f"Inserting {len(order_rows)} records into OrderDetail table in chunks...")
        
        # Insert in smaller chunks
        chunk_size = 50000  # Adjust this based on your system
        total_chunks = (len(order_rows) + chunk_size - 1) // chunk_size
        
        with conn.cursor() as cur:
            for chunk_num in range(total_chunks):
                start_idx = chunk_num * chunk_size
                end_idx = min((chunk_num + 1) * chunk_size, len(order_rows))
                chunk = order_rows[start_idx:end_idx]
                
                print(f"Inserting chunk {chunk_num + 1}/{total_chunks} ({len(chunk)} records)...")
                
                extras.execute_batch(
                    cur,
                    """
                    INSERT INTO OrderDetail
                    (CustomerID, ProductID, OrderDate, QuantityOrdered)
                    VALUES (%s, %s, %s, %s)
                    """,
                    chunk,
                    page_size=1000  # Smaller page size within each chunk
                )
                
                print(f"Chunk {chunk_num + 1} completed.")
        
        print(f"Successfully inserted all {len(order_rows)} records.")

    print("All tables created successfully!")


# Main execution
if __name__ == "__main__":
    data_filename = 'raw_product_data.csv'
    DATABASE_URL = get_db_url()
    
    # Drop all tables first
    drop_all_tables(DATABASE_URL)
    
    # Create all tables
    create_all_tables(data_filename, DATABASE_URL)