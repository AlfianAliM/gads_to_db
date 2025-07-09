import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# konfigurasi db
postgres_config = {
    "host": os.getenv("pg_host"),
    "port": int(os.getenv("pg_port")),
    "dbname": os.getenv("pg_dbname"),
    "user": os.getenv("pg_user"),
    "password": os.getenv("pg_password"),
}

# insert data
def insert_to_postgres(rows):
    conn = psycopg2.connect(**postgres_config)
    cur = conn.cursor()
    cur.executemany("""
        insert into gads_ad_group_ad_legacy_v2 (
            segments_date, campaign_id, campaign_name, ad_group_name,
            metrics_impressions, metrics_clicks, metrics_conversions,
            ad_group_ad_ad_strength, metrics_all_conversions,
            metrics_interaction_rate, ad_group_ad_ad_final_urls,
            metrics_cost_micros
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict do nothing;
    """, rows)
    conn.commit()
    cur.close()
    conn.close()
