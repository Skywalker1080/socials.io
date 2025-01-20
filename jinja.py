# prompt: write a code for collecting start time and end time and then the difference , in 3 lines
import time

start_time = time.time()


import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import os
import json
import gspread

# Load the Google credentials JSON from the environment variable
gcp_credentials_json = os.getenv('GCP_CREDENTIALS')

# Define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']


# Ensure the environment variable is set
if gcp_credentials_json:
    try:
        # Load credentials from the environment variable JSON
        credentials_dict = json.loads(gcp_credentials_json)
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        print("Credentials successfully loaded.")
    except Exception as e:
        print(f"Error: Failed to parse credentials from environment variable. {e}")
        exit(1)
else:
    print("Error: GCP_CREDENTIALS environment variable is not set.")
    exit(1)

# Authorize the credentials
gc = gspread.authorize(credentials)



from datetime import datetime

# Define the URL or Sheet ID
sheet_url = "https://docs.google.com/spreadsheets/d/1I3yIxy6-izhri_umggZM1EQEKsIqvJwi1WfAwtYiMx4/edit#gid=0"
sheet_id = sheet_url.split("/d/")[1].split("/")[0]  # Extract the Sheet ID

try:
    # Open the Google Sheet
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.get_worksheet(0)  # Access the first worksheet

    # Fetch all data from the sheet
    data = worksheet.get_all_records()

    # Check if the sheet has a 'Date' column
    if 'Date' not in data[0]:
        print("Error: 'Date' column not found in the sheet.")
        exit(1)

    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.now().strftime('%Y-%m-%d')

    # Extract the Date column from the data
    date_column = [row['Date'] for row in data]

    # Check if today's date exists in the Date column
    if today_date in date_column:
        print(f"Today's date ({today_date}) exists in the sheet. Carousel Already posted")
        exit(1)

        
    else:
        print(f"Today's date ({today_date}) does not exist in the sheet. ")
        print("Executing next block of code...")
        

except Exception as e:
    print(f"Error: {e}")
    exit(1)




## start jinja 
from flask import Flask, render_template, send_file
import pandas as pd
from sqlalchemy import create_engine


app = Flask(__name__)

# Database connection configuration
DB_CONFIG = {
    'host': '34.55.195.199',        # GCP PostgreSQL instance public IP
    'database': 'dbcp',             # Database name
    'user': 'yogass09',             # Username
    'password': 'jaimaakamakhya',   # Password
    'port': 5432                    # PostgreSQL default port
}

def get_gcp_engine():
    """Create and return a SQLAlchemy engine for the GCP PostgreSQL database."""
    connection_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
                     f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_url)

# Initialize the GCP engine
gcp_engine = get_gcp_engine()
def fetch_data_as_dataframe():
    """Fetch data from the 'coins' table and return as a Pandas DataFrame."""
    query_top_100 = """
      SELECT slug, cmc_rank, last_updated, symbol, price, percent_change24h, market_cap, last_updated
      FROM crypto_listings_latest_1000
      WHERE cmc_rank < 50
      """
    
    try:
        # Use gcp_engine to execute the query and fetch data as a DataFrame
        top_100_cc  = pd.read_sql_query(query_top_100, gcp_engine)
        # Convert market_cap to billions and round to 2 decimal places
        top_100_cc['market_cap'] = (top_100_cc['market_cap'] / 1_000_000_000).round(2)
        top_100_cc['price'] = (top_100_cc['price']).round(2)
        top_100_cc['percent_change24h'] = (top_100_cc['percent_change24h']).round(2)

        # Create a list of slugs from the top_100_crypto DataFrame
        slugs = top_100_cc['slug'].tolist()
        # Prepare a string for the IN clause
        slugs_placeholder = ', '.join(f"'{slug}'" for slug in slugs)

        # Construct the SQL query
        query_logos = f"""
        SELECT logo, slug FROM "FE_CC_INFO_URL"
        WHERE slug IN ({slugs_placeholder})
        """

        # Execute the query and fetch the data into a DataFrame
        logos_and_slugs = pd.read_sql_query(query_logos, gcp_engine)

        # Merge the two DataFrames on the 'slug' column
        top_100_cc = pd.merge(top_100_cc, logos_and_slugs, on='slug', how='left')
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        top_100_cc = pd.DataFrame()  # Return an empty DataFrame in case of error
    return top_100_cc

def fetch_for_3():
    """Fetch data from the 'coins' table and return as a Pandas DataFrame."""
    query_top_500 = """
      SELECT slug, cmc_rank, last_updated, symbol, price, percent_change24h, market_cap, last_updated
      FROM crypto_listings_latest_1000
      WHERE cmc_rank < 500
      """
    
    try:
        # Use gcp_engine to execute the query and fetch data as a DataFrame
        top_500_cc  = pd.read_sql_query(query_top_500, gcp_engine)
        # Convert market_cap to billions and round to 2 decimal places
        top_500_cc['market_cap'] = (top_500_cc['market_cap'] / 1_000_000_000).round(2)
        top_500_cc['price'] = (top_500_cc['price']).round(2)
        top_500_cc['percent_change24h'] = (top_500_cc['percent_change24h']).round(2)

        # Create a list of slugs from the top_100_crypto DataFrame
        slugs = top_500_cc['slug'].tolist()
        # Prepare a string for the IN clause
        slugs_placeholder = ', '.join(f"'{slug}'" for slug in slugs)

        # Construct the SQL query
        query_logos = f"""
        SELECT logo, slug FROM "FE_CC_INFO_URL"
        WHERE slug IN ({slugs_placeholder})
        """
        query_dmv = f"""
        SELECT *
        FROM "FE_DMV_SCORES"
        """
        
        # Execute the query and fetch data for dmv
        dmv = pd.read_sql_query(query_dmv, gcp_engine)

        # Execute the query and fetch the data into a DataFrame
        logos_and_slugs = pd.read_sql_query(query_logos, gcp_engine)

        # Merge the two DataFrames on the 'slug' column
        top_500_cc = pd.merge(top_500_cc, logos_and_slugs, on='slug', how='left')
        top_500_cc = pd.merge(top_500_cc, dmv, on='slug', how='left')

        top_500_cc['Durability_Score'] = (top_500_cc['Durability_Score']).round(2)
        top_500_cc['Momentum_Score'] = (top_500_cc['Momentum_Score']).round(2)
        top_500_cc['Valuation_Score'] = (top_500_cc['Valuation_Score']).round(2)
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        top_500_cc = pd.DataFrame()  # Return an empty DataFrame in case of error
    return top_500_cc

def fetch_for_4():
    """Fetch data from the 'coins' table and return as a Pandas DataFrame."""
    query_long_short = """
      SELECT
  "FE_DMV_ALL"."id",
  "FE_DMV_ALL"."slug",
  "FE_DMV_ALL"."name",
  "FE_DMV_ALL"."bullish",
  "FE_DMV_ALL"."bearish",
  "crypto_listings_latest_1000"."symbol",
  "crypto_listings_latest_1000"."percent_change24h",
  "crypto_listings_latest_1000"."percent_change7d",
  "crypto_listings_latest_1000"."percent_change30d",
  "crypto_listings_latest_1000"."cmc_rank",
  "crypto_listings_latest_1000"."price",
  "crypto_listings_latest_1000"."market_cap",
  "FE_CC_INFO_URL"."logo",
  "FE_RATIOS"."m_rat_alpha",
  "FE_RATIOS"."d_rat_beta",
  "FE_RATIOS"."m_rat_omega"
FROM
  "FE_DMV_ALL"
JOIN
  "crypto_listings_latest_1000"
ON
  "FE_DMV_ALL"."slug" = "crypto_listings_latest_1000"."slug"
JOIN
  "FE_CC_INFO_URL"
ON
  "FE_DMV_ALL"."slug" = "FE_CC_INFO_URL"."slug"
JOIN
  "FE_RATIOS"
ON
  "FE_DMV_ALL"."slug" = "FE_RATIOS"."slug"
WHERE
  "crypto_listings_latest_1000"."cmc_rank" < 100
ORDER BY
  "FE_DMV_ALL"."bullish" DESC
LIMIT
    10;
      """
    
    try:
        # Use gcp_engine to execute the query and fetch data as a DataFrame
        long_short  = pd.read_sql_query(query_long_short, gcp_engine)
        # Convert market_cap to billions and round to 2 decimal places
        long_short['market_cap'] = (long_short['market_cap'] / 1_000_000_000).round(2)
        long_short['price'] = (long_short['price']).round(2)
        long_short['percent_change24h'] = (long_short['percent_change24h']).round(2)
        long_short['percent_change7d'] = (long_short['percent_change7d']).round(2)
        long_short['percent_change30d'] = (long_short['percent_change30d']).round(2)

        long_short['m_rat_alpha'] = (long_short['m_rat_alpha']).round(2)
        long_short['d_rat_beta'] = (long_short['d_rat_beta']).round(2)
        long_short['m_rat_omega'] = (long_short['m_rat_omega']).round(2)
        
    
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        long_short = pd.DataFrame()  # Return an empty DataFrame in case of error
    return long_short

def fetch_for_4_short():
    """Fetch data from the 'coins' table and return as a Pandas DataFrame."""
    query_long_short = """
      SELECT
  "FE_DMV_ALL"."id",
  "FE_DMV_ALL"."slug",
  "FE_DMV_ALL"."name",
  "FE_DMV_ALL"."bullish",
  "FE_DMV_ALL"."bearish",
  "crypto_listings_latest_1000"."symbol",
  "crypto_listings_latest_1000"."percent_change24h",
  "crypto_listings_latest_1000"."percent_change7d",
  "crypto_listings_latest_1000"."percent_change30d",
  "crypto_listings_latest_1000"."cmc_rank",
  "crypto_listings_latest_1000"."price",
  "crypto_listings_latest_1000"."market_cap",
  "FE_CC_INFO_URL"."logo",
  "FE_RATIOS"."m_rat_alpha",
  "FE_RATIOS"."d_rat_beta",
  "FE_RATIOS"."m_rat_omega"
FROM
  "FE_DMV_ALL"
JOIN
  "crypto_listings_latest_1000"
ON
  "FE_DMV_ALL"."slug" = "crypto_listings_latest_1000"."slug"
JOIN
  "FE_CC_INFO_URL"
ON
  "FE_DMV_ALL"."slug" = "FE_CC_INFO_URL"."slug"
JOIN
  "FE_RATIOS"
ON
  "FE_DMV_ALL"."slug" = "FE_RATIOS"."slug"
WHERE
  "crypto_listings_latest_1000"."cmc_rank" < 100
ORDER BY
  "FE_DMV_ALL"."bearish" ASC
LIMIT
    10;
      """
    
    try:
        # Use gcp_engine to execute the query and fetch data as a DataFrame
        long_short  = pd.read_sql_query(query_long_short, gcp_engine)
        # Convert market_cap to billions and round to 2 decimal places
        long_short['market_cap'] = (long_short['market_cap'] / 1_000_000_000).round(2)
        long_short['price'] = (long_short['price']).round(2)
        long_short['percent_change24h'] = (long_short['percent_change24h']).round(2)
        long_short['percent_change7d'] = (long_short['percent_change7d']).round(2)
        long_short['percent_change30d'] = (long_short['percent_change30d']).round(2)

        long_short['m_rat_alpha'] = (long_short['m_rat_alpha']).round(2)
        long_short['d_rat_beta'] = (long_short['d_rat_beta']).round(2)
        long_short['m_rat_omega'] = (long_short['m_rat_omega']).round(2)
        
    
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        long_short = pd.DataFrame()  # Return an empty DataFrame in case of error
    return long_short
    

@app.route('/')
def display_coins():
    """Route to display coins data on an HTML page."""
    df = fetch_data_as_dataframe()
    df = df.sort_values('cmc_rank', ascending=True)
    df4 = fetch_for_4()
    
    # Convert DataFrame to list of dictionaries
    coins = df.to_dict(orient='records')
    return  render_template('index.html', coins=coins)

@app.route('/2')
def display_page2():
    """Route to display the second page with coins ranked 16-36, split into three equal parts."""
    df2 = fetch_data_as_dataframe()
    df2 = df2.sort_values('cmc_rank', ascending=True)
    
    # Skip first 15 rows and take next 21 rows (ranks 16-36)
    df2 = df2.iloc[15:36]
    
    # Split into three DataFrames of 7 rows each
    df2_part1 = df2.iloc[0:7]    # First 7 rows (ranks 16-22)
    df2_part2 = df2.iloc[7:14]   # Next 7 rows (ranks 23-29)
    df2_part3 = df2.iloc[14:21]  # Last 7 rows (ranks 30-36)
    
    # Convert each DataFrame to dictionary
    coins_part1 = df2_part1.to_dict(orient='records')
    coins_part2 = df2_part2.to_dict(orient='records')
    coins_part3 = df2_part3.to_dict(orient='records')
    
    return render_template('2.html', 
                         coins1=coins_part1, 
                         coins2=coins_part2, 
                         coins3=coins_part3)

@app.route('/3')
def display_page3():
    """Route to display the third page with coins ranked 37-57, split into three equal parts."""
    df3 = fetch_for_3()
    
    df3l = df3.sort_values('percent_change24h', ascending=True)  # top losers
    df3l = df3l.iloc[0:4]
    df3g = df3.sort_values('percent_change24h', ascending=False)  # top gainers
    df3g = df3g.iloc[0:4]  # get first 4 rows (highest percent change)

   

    df3l_new = df3l.to_dict(orient='records')
    df3g_new = df3g.to_dict(orient='records')
    
    
    return render_template('3.html', coin1=df3l_new, coin2=df3g_new)

@app.route('/4')
def display_page4():
    """Route to display the fourth page with long and short positions."""
    long_short = fetch_for_4()
    short = fetch_for_4_short()
    
    
    # Create DataFrame for long positions (sorted by bullish in descending order)
    df_long = long_short.sort_values('bullish', ascending=False)
    df_long = df_long.head(4)
    # Create DataFrame for short positions (sorted by bearish in ascending order)
    df_short = short.sort_values('bearish', ascending=True)
    df_short = df_short.head(4)

    
    
    # Convert DataFrames to dictionaries for template rendering
    long_positions = df_long.to_dict(orient='records')
    short_positions = df_short.to_dict(orient='records')
    print(short_positions)
    
    return render_template('4.html', coins1=long_positions, coins2=short_positions)

@app.route('/5')
def display_page5():
    html5 =  render_template('5.html')
    return html5

@app.teardown_appcontext
def shutdown_session(exception=None):
    """Dispose of the database connection after each request."""
    gcp_engine.dispose()

if __name__ == '__main__':
    app.run(debug=True)
































































## end jinja











"""## Caption Generator"""


# Google Spreadsheet details
spreadsheet_key = '1Ppif1y284fLPVIIoRzAXbPi9eUXzAyjOBr5DR-6XjSM'  # Replace with your spreadsheet key

# Open the Google Spreadsheet
sh = gc.open_by_key(spreadsheet_key)

# Get all sheet names
sheet_names = [sheet.title for sheet in sh.worksheets()]

# Loop through each sheet and load its data into a DataFrame
for sheet_name in sheet_names:
    worksheet = sh.worksheet(sheet_name)
    data = worksheet.get_all_values()  # Fetch all data from the sheet
    df = pd.DataFrame(data[1:], columns=data[0])  # Convert to DataFrame with headers
    globals()[sheet_name] = df  # Store DataFrame in a variable with the sheet name

# Convert percentage columns to numeric if they're in string format
Top50Coins['pct_1d_num'] = Top50Coins['pct_1d'].str.replace('%', '').astype(float)

# Get the top gainer and top loser
top_gainer = Top50Coins.loc[Top50Coins['pct_1d_num'].idxmax()]
top_loser = Top50Coins.loc[Top50Coins['pct_1d_num'].idxmin()]

# Extract necessary information
gainer_symbol = top_gainer['symbol']
gainer_pct = top_gainer['pct_1d']
loser_symbol = top_loser['symbol']
loser_pct = top_loser['pct_1d']


import pandas as pd

# Assuming BTC_SNAPSHOT is your DataFrame for Bitcoin
btc_row = BTC_SNAPSHOT.iloc[0]

# Extract necessary information
btc_slug = btc_row['slug']
btc_rank = btc_row['cmc_rank']
btc_symbol = btc_row['symbol']
btc_price = btc_row['price']
btc_percent_change_24h = btc_row['percent_change24h']
btc_volume_24h = btc_row['volume24h']
btc_market_cap = btc_row['market_cap']
btc_percent_change_7d = btc_row['percent_change7d']
btc_percent_change_30d = btc_row['percent_change30d']
btc_ytd_change = btc_row['ytd_price_change_percentage']
btc_last_updated = btc_row['last_updated']
btc_colour_24h = btc_row['colour_percent_change24h']
btc_bullish = btc_row['bullish']
btc_bearish = btc_row['bearish']
btc_neutral = btc_row['neutral']
btc_sentiment_diff = btc_row['sentiment_diff']
btc_trend = btc_row['Trend']

# Convert 'bearish_count' column to numeric
ShortOpportunities['bearish_count'] = pd.to_numeric(ShortOpportunities['bearish_count'])

# Get top 2 short opportunities based on bearish count
top_shorts = ShortOpportunities.nlargest(2, 'bearish_count')


LongOpportunities['bullish_count'] = pd.to_numeric(LongOpportunities['bullish_count'])

# Get top 2 long opportunities based on bullish count
top_longs = LongOpportunities.nlargest(2, 'bullish_count')

# Generate Instagram Caption with bullet points
caption = f"""

🚨 Crypto Opportunities Alert! 🚨

Crypto Market Overview:
- Today is {MarketOverview['Todays_Date'][0]}, {MarketOverview['Todays_Day'][0]} at {MarketOverview['Current_Time'][0]}!

* Market Stats:
  - Total Volume (24h): {MarketOverview['total_volume24h_reported'][0]} (Change: {MarketOverview['total_volume24h_yesterday_percentage_change'][0]}%)
  - Altcoin Volume (24h): {MarketOverview['altcoin_volume24h_reported'][0]}
  - Derivatives Volume (24h): {MarketOverview['derivatives_volume24h_reported'][0]} (Change: {MarketOverview['derivatives24h_percentage_change'][0]}%)

* DeFi Highlights:
  - DeFi Volume (24h): {MarketOverview['defi_volume24h_reported'][0]} (Change: {MarketOverview['defi24h_percentage_change'][0]}%)
  - DeFi Market Cap: {MarketOverview['defi_market_cap'][0]}

* Dominance Metrics:
  - Bitcoin Dominance: {MarketOverview['btc_dominance'][0]} (Change 24h: {MarketOverview['btc_dominance24h_percentage_change'][0]}%)
  - Ethereum Dominance: {MarketOverview['eth_dominance'][0]} (Change 24h: {MarketOverview['eth_dominance24h_percentage_change'][0]}%)

Bitcoin (BTC) Update:
- Rank: #{btc_rank}
- Current Price: {btc_price}
- 24h Change: {btc_percent_change_24h}% ({'🔴' if btc_colour_24h == 'red' else '🟢'} Update)
- 24h Volume: {btc_volume_24h}
- Market Cap: {btc_market_cap}
- Last Updated: {btc_last_updated}

* 7-Day Change: {btc_percent_change_7d}%
* 30-Day Change: {btc_percent_change_30d}%
* YTD Change: {btc_ytd_change}%

Market Sentiment:
- Bullish: {btc_bullish}
- Bearish: {btc_bearish}
- Neutral: {btc_neutral}
- Sentiment Diff: {btc_sentiment_diff}
- Current Trend: {btc_trend}

Crypto Highlights: Top 50 Coins
- Top Gainer: {gainer_symbol} skyrocketed by +{gainer_pct}%
- Top Loser: {loser_symbol} dropped by -{loser_pct}%

Top Short Squeeze Candidates:
1. {top_shorts.iloc[0]['slug']}
   - Bearish Count: {top_shorts.iloc[0]['bearish_count']}
   - Market Cap: {top_shorts.iloc[0]['market_cap']}
   - 24h Change: {top_shorts.iloc[0]['percent_change24h']}

2. {top_shorts.iloc[1]['slug']}
   - Bearish Count: {top_shorts.iloc[1]['bearish_count']}
   - Market Cap: {top_shorts.iloc[1]['market_cap']}
   - 24h Change: {top_shorts.iloc[1]['percent_change24h']}

Top Long Play Opportunities:
1. {top_longs.iloc[0]['slug']}
   - Bullish Count: {top_longs.iloc[0]['bullish_count']}
   - Market Cap: {top_longs.iloc[0]['market_cap']}
   - 24h Change: {top_longs.iloc[0]['percent_change24h']}

2. {top_longs.iloc[1]['slug']}
   - Bullish Count: {top_longs.iloc[1]['bullish_count']}
   - Market Cap: {top_longs.iloc[1]['market_cap']}
   - 24h Change: {top_longs.iloc[1]['percent_change24h']}

The crypto market never sleeps! Are you bullish or bearish? Let’s hear your thoughts!

#Crypto #LongAndShort #MarketMoves #InvestSmart #TradeWisely

Stay ahead of the game! 🚀💎
"""

print(caption)

# Install the necessary library

#!pip install together

# @title Caption Generator LLM

import os
import requests
from together import Together
from PIL import Image
import io
import base64


# Set your Together API key (replace with your actual key)
os.environ["TOGETHER_API_KEY"] = "3fe68043428d4e823a69fa534f0ee3cb8a355ff265fd9afba5ff5c48f7a7dc03" # Replace with your actual API key

client = Together()


response = client.chat.completions.create(
    model="google/gemma-2-27b-it",
    messages=[  {
                "role": "user",
                "content": f"Can you write 2200 character instagram caption summarzing my crypto market recap for the day using the data :{caption}"
                f"Make Sure it is well formatted, for instagram not as markdown so that user can read it on phone and has maximum information from :{caption}"
                f"We need to Make sure it starts with a FOMO hook  to nudge user to read the caption "
                f"and place random hooks and CTA it in captions at regular interval, to nudge user to , and follow us and like the post"
                f"Make sure the Caption is well spaced out and INformation is not Cluttered use indents and brackets whereever necessary"
                f"At begining of all headings and key data points Relevant Emojis are at the core pls make sure you encorporate them need to be colorfull and thoughtful "
                f"Ready to ship on instagram (NO AI comments in the output like here is your caption etc), my handle is @cryptoprism.io and my website is cryptoprism.io also link in bio"
                f"**-dont want this kind of formatting make sure the character count is under 2000"
        }],
    max_tokens= 1600,
    temperature=1,
    top_p=0.5,
    top_k=50,
    repetition_penalty=0.88,
    stop=["<|eot_id|>","<|eom_id|>"],
    stream=True
)
 #Initialize an empty string to store the output
caption1 = ""

for token in response:
    if hasattr(token, 'choices'):
        caption1 += token.choices[0].delta.content  # Append each token's content to caption1

# Now caption1 contains the complete output
print(caption1)  # Display the generated caption

"""# Instagram Bot"""

#!pip install instagrapi

import json
drive_service = build('drive', 'v3', credentials=credentials)

# List files
files = drive_service.files().list(pageSize=10).execute().get('files', [])
for file in files:
    print(file['name'], file['id'])

from googleapiclient.http import MediaIoBaseDownload
import io
# Replace with the desired file ID (from the printed list)
file_id = '1paoi_HhOMpFoafmfkIerHq4CH9Snq76i'
file_name = 'instagram_settings.json'  # Name to save the downloaded file

# Download the file
request = drive_service.files().get_media(fileId=file_id)
with io.FileIO(file_name, 'wb') as file:
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}% complete.")

print(f"File downloaded as {file_name}")

# Load the downloaded file content into a variable
with open(file_name, 'r') as f:
    instagram_login = json.load(f)

# Print or use the variable
print(instagram_login)

from instagrapi import Client
from pathlib import Path
import os
import io
import json
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Define paths
DRIVE_FILE_ID = '1paoi_HhOMpFoafmfkIerHq4CH9Snq76i'  # Replace with your file ID
LOCAL_PATH = 'instagram_settings.json'

# Function to download settings from Google Drive
def download_from_drive(file_id, local_path):
    request = drive_service.files().get_media(fileId=file_id)
    with io.FileIO(local_path, 'wb') as file:
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete.")

# Function to upload settings to Google Drive
def upload_to_drive(file_id, local_path):
    media = MediaFileUpload(local_path, mimetype='application/json')
    drive_service.files().update(fileId=file_id, media_body=media).execute()
    print(f"Uploaded {local_path} to Google Drive")

try:
    # Download settings from Drive if they exist
    download_from_drive(DRIVE_FILE_ID, LOCAL_PATH)
    print("Downloaded existing settings from Drive")

    # Load existing settings and verify session
    cl = Client()
    cl.load_settings(LOCAL_PATH)
    cl.get_timeline_feed()  # Verify session is still valid
    print("Successfully loaded existing session")

except Exception as e:
    print("Creating new session...")
    cl = Client()
    cl.login("cryptoprism.io", "jaimaakamakhya")
    cl.dump_settings(LOCAL_PATH)

    # Upload new settings to Drive
    upload_to_drive(DRIVE_FILE_ID, LOCAL_PATH)
    print(f"New session created and uploaded to Drive")

print("Session is ready to use")

media_files = [
    Path("1.jpg"),
    Path("2.jpg"),
    Path("3.jpg"),
    Path("4.jpg"),
    Path("5.jpg")
]


# Define the caption for your carousel post
caption = caption
# Upload the carousel post
media = cl.album_upload(media_files, caption1)



media_dict = media.model_dump() 

data=media_dict

# Extract relevant fields
carousel_info = {
    "Post ID": data['id'],
    "Code": data['code'],
    "Date": data['taken_at'].strftime('%Y-%m-%d') if isinstance(data['taken_at'], datetime) else None,
}

# Convert to a DataFrame (table structure)
df = pd.DataFrame([carousel_info])

sheet_name = 'Updates'

try:
    # Try to open the existing sheet
    try:
        sh = gc.open(sheet_name)
        print(f"Sheet '{sheet_name}' found and opened.")
    except gspread.exceptions.SpreadsheetNotFound:
        # If the sheet doesn't exist, create it
        sh = gc.create(sheet_name)
        print(f"Sheet '{sheet_name}' created successfully.")
        # Share the sheet with edit permissions (optional)
        sh.share('yogass09@gmail.com', perm_type='user', role='writer')  # Replace with your email

    # Select the first worksheet
    worksheet = sh.get_worksheet(0)  # Access the first worksheet

    # Check if headers exist, if not, add them
    if worksheet.row_count == 0 or worksheet.col_count == 0:
        worksheet.update([df.columns.values.tolist()])
        print("Headers added to the worksheet.")

    # Append rows to the worksheet
    worksheet.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
    print("New rows successfully appended to the worksheet.")

except Exception as e:
    print(f"Error: {e}")


# @title Time
#12:02
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time} seconds")
