import csv
import psycopg2
from psycopg2.extras import execute_values
import time


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_events",
    "user": "myuser",
    "password": "mypass"
}

CSV_FILE = "2019-Dec.csv"

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_last_offset(cursor):
    cursor.execute("SELECT last_offset FROM offset_metadata")
    row = cursor.fetchone()
    return row[0] if row else 0

def save_offset(cursor, offset):
    cursor.execute("""
        INSERT INTO offset_metadata(id, last_offset)
        VALUES (1, %s)
        ON CONFLICT (id) DO UPDATE SET last_offset = EXCLUDED.last_offset
    """, (offset,))

def insert_batch(cursor, batch):
    query = """
    INSERT INTO f_events (
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session
    ) VALUES %s;
    """
    execute_values(cursor, query, batch)


def process_csv(batch_size=10000, delay_between_batches=0, max_batches=1):
    conn = get_connection()
    cursor = conn.cursor()

    start_offset = get_last_offset(cursor)
    batch = []
    batch_count = 0
    current_line = 0

    with open(CSV_FILE, "r") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            current_line += 1
            if current_line <= start_offset:
                continue

            batch.append((
                row[0], row[1],
                int(row[2]) if row[2] else None,
                int(row[3]) if row[3] else None,
                row[4], row[5],
                float(row[6]) if row[6] else None,
                int(row[7]) if row[7] else None,
                row[8]
            ))

            if len(batch) >= batch_size:
                insert_batch(cursor, batch)
                save_offset(cursor, current_line)
                conn.commit()
                batch.clear()
                batch_count += 1
                if delay_between_batches > 0:
                    time.sleep(delay_between_batches)
                if max_batches is not None and batch_count >= max_batches:
                    print(f"Reached max_batches={max_batches}, stopping at line {current_line}.")
                    break

    #last batch
    if batch:
        insert_batch(cursor, batch)
        save_offset(cursor, current_line)
        conn.commit()

    cursor.close()
    conn.close()




if __name__ == "__main__":
    start_time = time.time()
    batch_size = 50
    delay_between_batches = 0
    max_batches = 2
    process_csv(batch_size=batch_size, delay_between_batches=delay_between_batches, max_batches=max_batches)
    end_time = time.time()
    print(f"Total time: {end_time - start_time:.6f} sec | Batch size : {batch_size}, Delay : {delay_between_batches}, Max Batches : {max_batches} ")