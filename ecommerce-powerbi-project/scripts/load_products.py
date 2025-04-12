import os
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

data_path = "C:/ecommerce-powerbi-project/Data"
chunksize = 100000


cur.execute("SELECT id, category_code FROM categories")
category_map = dict(cur.fetchall())

cur.execute("SELECT id, brand_name FROM brands")
brand_map = dict(cur.fetchall())

product_set = set()

for file in os.listdir(data_path):
    if file.endswith(".csv"):
        file_path = os.path.join(data_path, file)
        print(f"Processing {file}...")

        chunk_num = 0
        total_inserted = 0  
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk_num += 1
            chunk_inserted = 0  
            print(f"🔄 Processing chunk {chunk_num}...")

            for idx, row in chunk.iterrows():
                product_id = row["product_id"]
                category_code = row["category_code"]
                brand_name = row["brand"]
                price = row.get("price", None)

                if pd.isna(product_id) or product_id in product_set:
                    continue

                category_id = category_map.get(category_code)
                brand_id = brand_map.get(brand_name)

                if not category_id or not brand_id:
                    continue

                try:
                    cur.execute("""
                        INSERT INTO products (id, category_id, brand_id, price)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                    """, (product_id, category_id, brand_id, price))
                    product_set.add(product_id)
                    chunk_inserted += 1  # Increment for each row successfully inserted

                except Exception as e:
                    print(f"❌ Error inserting product {product_id}: {e}")

            conn.commit()
            total_inserted += chunk_inserted
            print(f"✅ Chunk {chunk_num} committed. {chunk_inserted} rows inserted in this chunk.")

        print(f"📊 Total rows inserted from {file}: {total_inserted}.")

cur.close()
conn.close()
print("✅ All products loaded successfully.")
