FROM python:3.10-slim

# Instalación de dependencias del sistema
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    pip install --upgrade pip && \
    pip install cryptography psycopg2 pandas boto3 sqlalchemy

# Copiar el código al contenedor
COPY . /app

# Establecer el directorio de trabajo
WORKDIR /app

# Comando para ejecutar el script
CMD ["python", "export_to_s3.py"]
