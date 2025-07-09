from ads import fetch_all_ads_data
from db import insert_to_postgres

# fungsi utama
def main():
    rows = fetch_all_ads_data()
    insert_to_postgres(rows)
    print(f"{len(rows)} rows inserted to gads_ad_group_ad_legacy_v2.")

# eksekusi
if __name__ == "__main__":
    main()



