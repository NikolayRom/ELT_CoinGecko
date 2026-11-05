import json
import os
from datetime import datetime, timedelta
import boto3
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

CRYPTO_LIST = [
    "bitcoin",
    "ethereum",
    "tether",
    "binancecoin",
    "ripple",
    "usd-coin",
    "solana",
    "tron",
    "figure-heloc",
    "zcash",
    "hyperliquid",
    "dogecoin",
    "monero",
    "usds",
    "whitebit",
    "chainlink",
    "leo-token",
    "cardano",
    "near",
    "stellar",
    "bitcoin-cash",
    "rain",
    "ethena-usde",
    "dai",
    "uniswap",
    "litecoin",
    "avalanche-2",
    "canton-network",
    "usd1-wlfi",
    "sui",
    "the-open-network",
    "hedera-hashgraph",
    "tether-gold",
    "global-dollar",
    "paypal-usd",
    "bittensor",
    "quant-network",
    "shiba-inu",
    "ethena",
    "crypto-com-chain",
    "ripple-usd",
    "memecore",
    "bitway",
    "ondo-us-dollar-yield",
    "hashnote-usyc",
    "okb",
    "blackrock-usd-institutional-digital-liquidity-fund",
    "aave",
    "ondo-finance",
    "pump-fun",
    "pepe",
    "falcon-finance",
    "pax-gold",
    "spiko-amundi-overnight-swap-fund-eur",
    "aster-2",
    "usdd",
    "mantle",
    "polkadot",
    "world-liberty-financial",
    "worldcoin-wld",
    "united-stables",
    "morpho",
    "sky",
    "htx-dao",
    "internet-computer",
    "bfusd",
    "bitget-token",
    "usdgo",
    "venice-token",
    "ethereum-classic",
    "arbitrum",
    "just",
    "kucoin-shares",
    "gatechain-token",
    "polygon-ecosystem-token",
    "kaspa",
    "algorand",
    "jupiter-exchange-solana",
    "render-token",
    "cosmos",
    "pi-network",
    "open-usd",
    "injective-protocol",
    "nexo",
    "lighter",
    "filecoin",
    "superstate-short-duration-us-government-securities-fund-ustb",
    "dash",
    "gho",
    "xdce-crowd-sale",
    "pancakeswap-token",
    "midnight-3",
    "layerzero",
    "ether-fi",
    "pearl-2",
    "aptos",
    "beldex",
    "pyth-network",
    "blockstack",
    "janus-henderson-anemoy-aaa-clo-fund",
]

CRYPTO_IDS = ",".join(CRYPTO_LIST)

BUCKET_NAME = os.getenv("BUCKET_NAME", "crypto-bronze")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
COINGECKO_KEY = os.getenv("COINGECKO_API_KEY", "")

def extract_and_load_to_s3(**context):
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?vs_currencies=usd"
        f"&ids={CRYPTO_IDS}"
        f"&include_market_cap=true"
        f"&include_last_updated_at=true"
    )

    headers = {}
    if COINGECKO_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_KEY

    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    raw_data = response.json()

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name="us-east-1"
    )

    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
    except Exception:
        pass

    now = datetime.utcnow()
    file_key = (
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"crypto_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    payload = {
        "extracted_at": now.isoformat(),
        "data": raw_data
    }

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=json.dumps(payload, ensure_ascii=False),
        ContentType="application/json"
    )

    print(f"Successfully uploaded raw data to s3://{BUCKET_NAME}/{file_key}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id='extract_crypto',
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    schedule=timedelta(minutes=5),
    max_active_runs=1,
    catchup=False,
    tags=['crypto', 'extract', 'load', 's3'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_coingecko_to_s3',
        python_callable=extract_and_load_to_s3,
    )