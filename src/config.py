import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente (se existirem no .env)
load_dotenv()

# Configurações da API CoinGecko
# Usaremos o endpoint 'markets' para pegar preço, cap de mercado e volume
BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"
VS_CURRENCY = "usd"
COIN_IDS = ["bitcoin", "ethereum"] # IDs das criptomoedas a serem monitoradas

# Configurações de Retry
MAX_RETRIES = 3
TIMEOUT_SECONDS = 10