import os
import pandas as pd
import psycopg2

data_path="C:/ecommerce-powerbi-project/Data"

db_config = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user":"postgres",
    "password":"1234"
}

def load_users():
    conn=psycopg2.connect(**db_config)
    cursor=conn.cursor()
    unique_users=set()

    for file in os.listdir(data_path):
        if file.endswith(".csv"):
            file_path = os.path.join(data_path,file)
            df = pd.read_csv(file_path)
            unique_users.update(df['user_id'].dropna().unique())

    print(f"Total unique users: {len(unique_users)}")

    insert_query = "INSERT INTO users (id) VALUES (%s) ON CONFLICT DO NOTHING"

    for user_id in unique_users:
        try:
            cursor.execute(insert_query,(int(user_id),))
        except Exception as e:
            print(f"Error inserting user_id {user_id}:{e}")
        
    conn.commit()
    cursor.close()
    conn.close()
    print("Users loaded successfully!")

if __name__=="__main__":
    load_users()