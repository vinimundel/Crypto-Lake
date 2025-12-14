from src.extract import fetch_crypto_data
from src.transform import process_data, save_to_parquet
from src.load import upload_to_s3  # <--- Import novo
import pandas as pd
import os
from dotenv import load_dotenv

# Garante que as vars do .env sejam carregadas no início
load_dotenv()

if __name__ == "__main__":
    print("=== Pipeline Crypto-Lake Iniciado ===")
    
    try:
        # 1. Extract
        raw_data = fetch_crypto_data()
        
        # 2. Transform
        df = process_data(raw_data)
        
        timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"crypto_data_{timestamp_str}.parquet"
        
        # Salva localmente primeiro (buffer)
        local_path = save_to_parquet(df, filename)
        
        # 3. Load (Upload to S3)
        if local_path:
            success = upload_to_s3(local_path, s3_folder="processed")
            if success:
                print("Pipeline finalizado: Dados na Nuvem! ☁️")
            else:
                print("Pipeline finalizado com ERRO no Upload.")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")