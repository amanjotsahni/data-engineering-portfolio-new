import psycopg2

conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

csv_file_path = "C:/ecommerce-powerbi-project/Data/my_data.csv"

# Execute COPY command to load data from CSV file into PostgreSQL
try:
    cur.execute("""
        COPY categories (category_id, category_code)
        FROM %s
        DELIMITER ','
        CSV HEADER;
    """, (csv_file_path,))
    conn.commit()
    print("✅ Data loaded successfully into categories table.")
except Exception as e:
    conn.rollback()
    print(f"❌ Error loading data: {e}")

finally:
    cur.close()
    conn.close()
