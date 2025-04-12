import os
import pandas as pd
import psycopg2

# PostgreSQL connection setup
conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",  # replace with actual password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Load foreign key sets from dimension tables
cur.execute("SELECT product_id FROM products")
product_ids = {row[0] for row in cur.fetchall()}

cur.execute("SELECT user_id FROM users")
user_ids = {row[0] for row in cur.fetchall()}

cur.execute("SELECT session_id FROM sessions")
session_ids = {row[0] for row in cur.fetchall()}

data_path = "C:/ecommerce-powerbi-project/Data"
chunksize = 100000
event_set = set()

for file in os.listdir(data_path):
    if file.endswith(".csv"):
        file_path = os.path.join(data_path, file)
        print(f"\n📂 Processing file: {file}...")

        chunk_num = 0
        total_events_inserted = 0

        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk_num += 1
            chunk_events_inserted = 0
            print(f"🔄 Processing chunk {chunk_num}...")

            for idx, row in chunk.iterrows():
                event_time = row["event_time"]
                event_type = row["event_type"]
                product_id = row["product_id"]
                user_id = row["user_id"]
                session_id = row["user_session"]
                price = row.get("price", None)

                if pd.isna(event_time) or pd.isna(event_type) or pd.isna(product_id) or pd.isna(user_id) or pd.isna(session_id):
                    continue

                # Skip if foreign keys don't exist
                if product_id not in product_ids:
                    print(f"⚠️ Skipping row {idx+1}: product_id {product_id} not found.")
                    continue
                if user_id not in user_ids:
                    print(f"⚠️ Skipping row {idx+1}: user_id {user_id} not found.")
                    continue
                if session_id not in session_ids:
                    print(f"⚠️ Skipping row {idx+1}: session_id {session_id} not found.")
                    continue

                event_key = (event_time, event_type, product_id, user_id, session_id)
                if event_key in event_set:
                    continue

                try:
                    cur.execute("""
                        INSERT INTO fact_events (event_time, event_type, product_id, user_id, session_id, price)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (event_time, event_type, product_id, user_id, session_id, price))
                    event_set.add(event_key)
                    chunk_events_inserted += 1

                except Exception as e:
                    conn.rollback()  # Roll back this single failed insert
                    print(f"❌ Error inserting event at row {idx+1} in chunk {chunk_num}: {e}")
                    print("🚨 Transaction rolled back. Stopping further inserts.")
                    cur.close()
                    conn.close()
                    exit(1)

            conn.commit()
            total_events_inserted += chunk_events_inserted
            print(f"✅ Chunk {chunk_num} committed. {chunk_events_inserted} events inserted.")

        print(f"📊 Total events inserted in {file}: {total_events_inserted} events.")

cur.close()
conn.close()
print("\n✅ All events loaded successfully.")
