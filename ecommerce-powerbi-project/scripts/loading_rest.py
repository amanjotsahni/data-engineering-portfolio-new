import psycopg2

# Database connection config
conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="1234",  # replace with actual password
    host="localhost",
    port="5432"  # or your actual port
)
conn.autocommit = True
cur = conn.cursor()

while True:
    insert_query = """
    WITH to_insert AS (
        SELECT 
            sc.user_id,
            sc.user_session::uuid AS session_id,
            sc.event_type,
            sc.event_time,
            sc.product_id,
            sc.price,
            sc.category_id
        FROM staging_categories sc
        JOIN products p ON sc.product_id = p.product_id
        JOIN categories c ON sc.category_id = c.category_id
        JOIN users u ON sc.user_id = u.user_id
        JOIN sessions s ON sc.user_session::uuid = s.session_id
        WHERE sc.event_type IN ('view', 'cart', 'purchase')
          AND sc.event_time >= '2019-11-14'
          AND sc.event_time < '2019-12-01'
          AND NOT EXISTS (
              SELECT 1 
              FROM fact_events fe
              WHERE fe.user_id = sc.user_id
                AND fe.session_id = sc.user_session::uuid
                AND fe.event_time = sc.event_time
                AND fe.product_id = sc.product_id
          )
        LIMIT 10
    )
    INSERT INTO fact_events (
        user_id, session_id, event_type, event_time, product_id, price, category_id
    )
    SELECT * FROM to_insert;
    """

    cur.execute(insert_query)
    rows_inserted = cur.rowcount
    print(f"Inserted {rows_inserted} rows")

    if rows_inserted == 0:
        print("No more rows to insert.")
        break

cur.close()
conn.close()
