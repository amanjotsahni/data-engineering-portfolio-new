import os
import pandas as pd
import psycopg2

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

data_path = "C:/ecommerce-powerbi-project/Data"
chunksize = 100000
unique_brands = set()

# Go through all CSV files
total_files = len([f for f in os.listdir(data_path) if f.endswith(".csv")])
file_count = 0

for file in os.listdir(data_path):
    if file.endswith(".csv"):
        file_count += 1
        file_path = os.path.join(data_path, file)
        print(f"📁 Processing {file} ({file_count}/{total_files})...")

        chunk_num = 0
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk_num += 1
            print(f"🔄 Processing chunk {chunk_num} in file {file}...")

            chunk = chunk.dropna(subset=["brand"])

            for brand in chunk["brand"].unique():
                brand = brand.strip()
                if brand not in unique_brands:
                    try:
                        cursor.execute("""
                            INSERT INTO brands (brand_name)
                            VALUES (%s)
                            ON CONFLICT (brand_name) DO NOTHING;
                        """, (brand,))
                        unique_brands.add(brand)
                    except Exception as e:
                        print(f"❌ Error inserting brand {brand}: {e}")

            conn.commit()
            print(f"✅ Chunk {chunk_num} committed in file {file}.")

print("✅ All brands loaded successfully.")

cursor.close()
conn.close()
