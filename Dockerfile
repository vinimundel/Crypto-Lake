# Imagem Base: Python leve (Slim) baseada em Linux Debian
FROM python:3.9-slim

# Define diretório de trabalho dentro do container
WORKDIR /app

# Variáveis de ambiente para evitar arquivos .pyc e buffer de log
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instala dependências do sistema (necessário para alguns pacotes Python)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas o requirements primeiro (para aproveitar o cache do Docker)
COPY requirements.txt .

# Instala as libs Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código fonte e estrutura de pastas
COPY . .

# Cria as pastas de dados dentro do container (para o script não falhar ao salvar localmente)
RUN mkdir -p data/raw data/processed

# Comando padrão ao rodar o container
CMD ["python", "main.py"]