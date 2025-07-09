from google.ads.googleads.client import GoogleAdsClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

# konfigurasi token google ads
ads_config = {
    "developer_token": os.getenv("developer_token"),
    "client_id": os.getenv("client_id"),
    "client_secret": os.getenv("client_secret"),
    "refresh_token": os.getenv("refresh_token"),
    "customer_id": os.getenv("customer_id")
}

# inisialisasi klien google ads
client = GoogleAdsClient.load_from_dict({
    "developer_token": ads_config["developer_token"],
    "client_id": ads_config["client_id"],
    "client_secret": ads_config["client_secret"],
    "refresh_token": ads_config["refresh_token"],
    "login_customer_id": ads_config["customer_id"],
    "use_proto_plus": True  
})

# query ambil data
def fetch_ads_data():
    query = """
    select
      segments.date,
      campaign.id,
      campaign.name,
      ad_group.name,
      metrics.impressions,
      metrics.clicks,
      metrics.conversions,
      ad_group_ad.ad_strength,
      metrics.all_conversions,
      metrics.interaction_rate,
      ad_group_ad.ad.final_urls,
      metrics.cost_micros
    from ad_group_ad
    where segments.date during yesterday
    """

    # running query
    service = client.get_service("GoogleAdsService")
    response = service.search_stream(customer_id=ads_config["customer_id"], query=query)

    # memproses respon
    rows = []
    for batch in response:
        for row in batch.results:
            rows.append((
            str(row.segments.date),
            row.campaign.id,
            row.campaign.name,
            row.ad_group.name,
            int(row.metrics.impressions),
            int(row.metrics.clicks),
            float(row.metrics.conversions) if row.metrics.conversions is not None else 0,
            row.ad_group_ad.ad_strength.name,
            float(row.metrics.all_conversions) if row.metrics.all_conversions is not None else 0,
            float(row.metrics.interaction_rate) if row.metrics.interaction_rate is not None else 0,
            json.dumps(list(row.ad_group_ad.ad.final_urls)) if row.ad_group_ad.ad.final_urls else '[]',
            int(row.metrics.cost_micros)
        ))
    return rows
