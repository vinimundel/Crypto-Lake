import requests
import logging
from typing import List, Dict, Any
from src.config import BASE_URL, VS_CURRENCY, COIN_IDS, TIMEOUT_SECONDS

# Configuração básica de logs (essencial para debugar em produção/Docker)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_crypto_data() -> List[Dict[str, Any]]:
    """
    Busca dados de mercado para as criptomoedas configuradas.
    Retorna uma lista de dicionários (Raw Data).
    """
    params = {
        'vs_currency': VS_CURRENCY,
        'ids': ','.join(COIN_IDS),
        'order': 'market_cap_desc',
        'sparkline': 'false'
    }

    try:
        logging.info(f"Iniciando extração de dados para: {COIN_IDS}")
        response = requests.get(BASE_URL, params=params, timeout=TIMEOUT_SECONDS)
        
        # Levanta exceção se o status code for 4xx ou 5xx
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            logging.warning("API retornou uma lista vazia.")
            return []
            
        logging.info(f"Sucesso! {len(data)} registros extraídos.")
        return data

    except requests.exceptions.HTTPError as err:
        logging.error(f"Erro HTTP na requisição: {err}")
        raise
    except requests.exceptions.ConnectionError:
        logging.error("Erro de Conexão. Verifique sua internet ou DNS.")
        raise
    except requests.exceptions.Timeout:
        logging.error("A requisição excedeu o tempo limite (Timeout).")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
        raise