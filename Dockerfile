# Use uma imagem base com Python 3 e cron instalados
FROM python:3.9-slim

# Instale o cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie os arquivos e diretórios necessários para o contêiner
# Copie o requirements.txt primeiro para otimizar o cache do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie o script principal e a pasta de modelos
COPY main.py .
COPY models ./models

# Copie o arquivo de crontab e o script de inicialização
COPY crontab /etc/cron.d/meu-cronjob
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Dê permissão de execução ao script de inicialização
RUN chmod +x /usr/local/bin/entrypoint.sh

# Crie o arquivo de log para o cron
RUN touch /var/log/cron.log

# Dê permissão de escrita ao arquivo de log
RUN chmod 666 /var/log/cron.log

# Use o entrypoint para iniciar o cron e o script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
