import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
import os

# Configuração de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_data(raw_data: list) -> pd.DataFrame:
    """
    Transforma dados brutos em DataFrame, calcula métricas e enriquece os dados.
    """
    if not raw_data:
        logging.warning("Nenhum dado para processar.")
        return pd.DataFrame()

    try:
        # 1. Criação do DataFrame
        df = pd.DataFrame(raw_data)
        
        # Selecionar apenas colunas relevantes para o Lake (schema evolution prevention)
        cols_to_keep = [
            'id', 'symbol', 'name', 'current_price', 
            'market_cap', 'total_volume', 'high_24h', 'low_24h', 'last_updated'
        ]
        df = df[cols_to_keep]

        # 2. Engenharia de Features (Lógica do Z-Score)
        # Estimando média e desvio padrão baseados no range de 24h
        df['mean_24h'] = (df['high_24h'] + df['low_24h']) / 2
        df['std_dev_approx'] = (df['high_24h'] - df['low_24h']) / 4
        
        # Evitar divisão por zero (caso raro onde High == Low)
        df['std_dev_approx'] = df['std_dev_approx'].replace(0, np.nan)
        
        # Cálculo do Z-Score
        df['z_score'] = (df['current_price'] - df['mean_24h']) / df['std_dev_approx']
        
        # Flag de Anomalia (Se desvio for maior que 2 sigma)
        df['is_anomaly'] = df['z_score'].abs() > 2

        # 3. Adicionar Metadados de Engenharia
        # Usamos UTC, sempre. Nunca use hora local em Data Engineering.
        df['ingestion_timestamp'] = datetime.now(timezone.utc)
        
        logging.info(f"Transformação concluída. Shape: {df.shape}")
        return df

    except Exception as e:
        logging.error(f"Erro na transformação: {e}")
        raise

def save_to_parquet(df: pd.DataFrame, filename: str = "crypto_data.parquet"):
    """
    Salva o DataFrame em formato Parquet na pasta processed.
    Usa caminhos absolutos baseados na localização do projeto para evitar erros.
    """
    try:
        # 1. Descobre a raiz do projeto dinamicamente
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        
        # 2. Monta o caminho absoluto para a pasta de dados
        folder_path = os.path.join(project_root, "data", "processed")
        
        # 3. Defensive Programming: Cria a pasta se ela não existir
        os.makedirs(folder_path, exist_ok=True)
        
        # 4. Define o caminho completo do arquivo
        output_path = os.path.join(folder_path, filename)
        
        # Engine pyarrow é mais rápida e eficiente para Parquet
        df.to_parquet(output_path, index=False, engine='pyarrow')
        
        logging.info(f"Arquivo salvo com sucesso em: {output_path}")
        return output_path
    except Exception as e:
        logging.error(f"Erro ao salvar Parquet: {e}")
        raise