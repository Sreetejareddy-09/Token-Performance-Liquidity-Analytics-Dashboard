# load_crypto_to_snowflake.py
# ---------------------------
# Step-by-step script to fetch Ethereum data and load into Snowflake
# For VS Code / Windows
# ---------------------------

try:
    import requests
    import pandas as pd
    import snowflake.connector
    from datetime import datetime

    print("Step 1: Starting script")

    # ---------------------------
    # 1. Get Ethereum market data (last 30 days)
    # ---------------------------
    url = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart"
    params = {"vs_currency": "usd", "days": "30"}

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"CoinGecko API request failed: {response.status_code}")
    print("Step 2: Data fetched from CoinGecko")

    data = response.json()
    prices = data["prices"]
    volumes = data["total_volumes"]
    market_caps = data["market_caps"]

    rows = []
    for i in range(len(prices)):
        date = datetime.fromtimestamp(prices[i][0] / 1000).date()
        rows.append([date, prices[i][1], volumes[i][1], market_caps[i][1]])

    df = pd.DataFrame(rows, columns=["date", "price_usd", "volume_usd", "market_cap_usd"])
    print(f"Step 3: DataFrame created with {len(df)} rows")

    # ---------------------------
    # 2. Connect to Snowflake
    # ---------------------------
    conn = snowflake.connector.connect(
        user="SREETEJA0908",             # your Snowflake username
        password="Sreetejareddy@0908",   # your Snowflake password
        account="onnqpxp-ovc21017",      # your account (from Snowflake URL)
        warehouse="COMPUTE_WH",          # replace with your warehouse name
        database="CRYPTO_DB",
        schema="ANALYTICS",
        role="ACCOUNTADMIN"
    )
    print("Step 4: Connected to Snowflake")

    cursor = conn.cursor()

    # ---------------------------
    # 3. Insert data
    # ---------------------------
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO CRYPTO_DB.ANALYTICS.TOKEN_METRICS
            (date, price_usd, volume_usd, market_cap_usd)
            VALUES (%s, %s, %s, %s)
            """,
            (row["date"], row["price_usd"], row["volume_usd"], row["market_cap_usd"])
        )

    cursor.close()
    conn.close()
    print("✅ Data loaded successfully")

except Exception as e:
    print("❌ Error:", e)

# ---------------------------
# Keep console open in VS Code / Windows
# ---------------------------
input("Press Enter to exit...")
