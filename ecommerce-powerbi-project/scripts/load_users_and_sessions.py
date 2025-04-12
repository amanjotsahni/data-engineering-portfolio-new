import os
import pandas as pd
import psycopg2

# Establish DB connection
conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",  # replace with actual password
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Data path
data_path = "C:/ecommerce-powerbi-project/Data"
all_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]

chunksize = 100000
user_batch = []
session_batch = []
batch_size = 1000  # Size of the batch insert

for file in all_files:
    file_path = os.path.join(data_path, file)
    print(f"Processing file: {file}")

    chunk_count = 0
    total_user_inserted = 0  
    total_session_inserted = 0  

    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        chunk_count += 1
        chunk_user_inserted = 0  
        chunk_session_inserted = 0  
        print(f"🔄 Processing chunk {chunk_count}...")

        for _, row in chunk.iterrows():
            user_id = row.get("user_id")
            session_id = row.get("user_session")

            if pd.isna(user_id) or pd.isna(session_id):
                continue

            # Batch insert unique user
            user_batch.append((user_id,))
            if len(user_batch) >= batch_size:
                try:
                    cur.executemany(""" 
                        INSERT INTO users (user_id)
                        VALUES (%s)
                        ON CONFLICT (user_id) DO NOTHING
                    """, user_batch)
                    conn.commit()
                    total_user_inserted += len(user_batch)
                    chunk_user_inserted += len(user_batch)  # Track inserted for this chunk
                    user_batch.clear()
                except Exception as e:
                    print(f"❌ Error inserting batch of users: {e}")

            # Batch insert unique session
            session_batch.append((session_id,))
            if len(session_batch) >= batch_size:
                try:
                    cur.executemany(""" 
                        INSERT INTO sessions (session_id)
                        VALUES (%s)
                        ON CONFLICT (session_id) DO NOTHING
                    """, session_batch)
                    conn.commit()
                    total_session_inserted += len(session_batch)
                    chunk_session_inserted += len(session_batch)  # Track inserted for this chunk
                    session_batch.clear()
                except Exception as e:
                    print(f"❌ Error inserting batch of sessions: {e}")

        # Commit any remaining data for this chunk
        if user_batch:
            try:
                cur.executemany(""" 
                    INSERT INTO users (user_id)
                    VALUES (%s)
                    ON CONFLICT (user_id) DO NOTHING
                """, user_batch)
                conn.commit()
                total_user_inserted += len(user_batch)
                chunk_user_inserted += len(user_batch)
                user_batch.clear()
            except Exception as e:
                print(f"❌ Error inserting remaining users: {e}")

        if session_batch:
            try:
                cur.executemany(""" 
                    INSERT INTO sessions (session_id)
                    VALUES (%s)
                    ON CONFLICT (session_id) DO NOTHING
                """, session_batch)
                conn.commit()
                total_session_inserted += len(session_batch)
                chunk_session_inserted += len(session_batch)
                session_batch.clear()
            except Exception as e:
                print(f"❌ Error inserting remaining sessions: {e}")

        # Print chunk progress
        print(f"✅ Chunk {chunk_count} committed. "
              f"{chunk_user_inserted} users and {chunk_session_inserted} sessions inserted.")

    # Print total inserted per file
    print(f"📊 Total users and sessions inserted in {file}: {total_user_inserted} users, "
          f"{total_session_inserted} sessions.")

# Close DB connection
cur.close()
conn.close()
print("✅ All users and sessions loaded successfully.")
