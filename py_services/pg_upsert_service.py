

#################################################################################################
### -- > NOTE SPARK JOB IS NOT CONFIGURED TO HANDLE UPSERT EFFICIENTLY, USE INSERT SERVICE ! ####
#################################################################################################

import random
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_events",
    "user": "myuser",
    "password": "mypass"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def run(n_updates=500):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM f_events")
    total_rows = cursor.fetchone()[0]

    if total_rows == 0:
        print("No data in table yet.")
        cursor.close()
        conn.close()
        return

    n = min(n_updates, total_rows)
    print(f"Total rows: {total_rows}. Updating {n} rows...")

    cursor.execute("""
        SELECT ctid FROM f_events
        ORDER BY random()
        LIMIT %s
    """, (n,))
    ctids = [row[0] for row in cursor.fetchall()]

    for ctid in ctids:
        cursor.execute("""
            UPDATE f_events
            SET price = ROUND((price * %s)::numeric, 2),
                event_type = %s
            WHERE ctid = %s
        """, (
            round(random.uniform(0.85, 1.15), 4),
            random.choice(["view", "cart", "purchase", "remove_from_cart"]),
            ctid
        ))

    conn.commit()
    print(f"Updated {len(ctids)} rows.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    run(n_updates=3)