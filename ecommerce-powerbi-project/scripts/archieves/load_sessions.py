import os
import pandas as pd
import psycopg2

data_path="C:/ecommerce-powerbi-project/Data"

db_config = {
    "host":"localhost",
    "database":"ecommerce_db",
    "user": "postgres",
    "password":"1234"
}

unique_sessions=set()

for file in os.listdir(data_path):
    if file.endswith(".csv"):
        file_path = os.path.join(data_path,file)
        df = pd.read_csv(file_path)
        unique_sessions.update(df['user_session'].dropna().unique())

    
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    insert_query = "INSERT INTO sessions(session_id) VALUES (%s) ON CONFLICT DO NOTHING"

    for session_id in unique_sessions:
        cursor.execute(insert_query,(session_id,))

    conn.commit()
    print(f"Inserted {len(unique_sessions)} sessions successfully!")

except Exception as e:
    print("Error",e)

finally:
    cursor.close()
    conn.close()