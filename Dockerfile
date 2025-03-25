# Usa uma imagem oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Copiar apenas o arquivo de dependências primeiro (melhora cache)
COPY requirements.txt /app/

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia os arquivos do projeto
COPY . /app

# Criar usuário não root por segurança
RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Definir o ponto de entrada do container
ENTRYPOINT ["python", "report_ss.py"]