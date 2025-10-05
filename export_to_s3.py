import os
import pandas as pd
from sqlalchemy import create_engine
import boto3
import json

# ================================
# 🔧 CONFIGURACIÓN CON VARIABLES DE ENTORNO
# ================================

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]

# Bucket S3
BUCKET_NAME = os.getenv("BUCKET_NAME", "my-bucket")

# Diccionario de tablas y carpetas
TABLES = {
    "users": "users_folder/",
    "admin_profiles": "admin_profiles_folder/"
}

# Schemas para castear tipos
SCHEMAS = {
    "users": [
        {"Name": "id", "Type": "int"},
        {"Name": "email", "Type": "string"},
        {"Name": "password", "Type": "string"},
        {"Name": "firstname", "Type": "string"},
        {"Name": "lastname", "Type": "string"},
        {"Name": "username", "Type": "string"},
        {"Name": "role", "Type": "string"},
        {"Name": "creation_date", "Type": "date"},
        {"Name": "created_at", "Type": "timestamp"},
        {"Name": "updated_at", "Type": "timestamp"},
    ],
    "admin_profiles": [
        {"Name": "id", "Type": "int"},
        {"Name": "admision_to_admin_date", "Type": "date"},
        {"Name": "admision_to_admin_time", "Type": "string"},
        {"Name": "total_questions_answered", "Type": "int"},
        {"Name": "is_active", "Type": "string"},  # Booleano lo convertimos a string (true/false)
        {"Name": "created_at", "Type": "timestamp"},
        {"Name": "updated_at", "Type": "timestamp"},
    ]
}

# ================================
# 🚀 LÓGICA
# ================================

# Conexión PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Cliente S3
s3 = boto3.client("s3")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia strings y asegura compatibilidad con Athena"""
    for col in df.select_dtypes(include=["object", "bool"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[\r\n\t]", " ", regex=True)
            .str.replace(r"[^\x20-\x7E]", "", regex=True)
        )
    return df


def cast_types(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """Convierte columnas a los tipos definidos en schema"""
    for col in schema:
        name, typ = col["Name"], col["Type"]
        if name not in df.columns:
            continue
        if typ == "int":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif typ == "date":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d")
        elif typ == "timestamp":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        else:  # string
            df[name] = df[name].astype(str)
    return df


def export_to_ndjson(df: pd.DataFrame, filename: str):
    """Exporta a NDJSON (una fila = un JSON en una sola línea)"""
    with open(filename, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")


def main():
    # 1. Limpiar bucket
    print("🔄 Limpiando bucket...")
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                if obj["Key"].endswith(".json"):
                    s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
            print("✅ Archivos previos eliminados.")
    except Exception as e:
        print(f"⚠️ Error al limpiar bucket: {e}")

    # 2. Exportar tablas
    for table, folder in TABLES.items():
        try:
            print(f"📥 Exportando tabla: {table}")
            df = pd.read_sql(f"SELECT * FROM {table}", engine)

            df = clean_dataframe(df)
            df = cast_types(df, SCHEMAS[table])

            filename = f"{table}.json"
            export_to_ndjson(df, filename)

            s3_key = f"{folder}{filename}"
            print(f"⬆️ Subiendo {filename} a s3://{BUCKET_NAME}/{s3_key}")
            s3.upload_file(filename, BUCKET_NAME, s3_key)
            print(f"✅ {filename} subido correctamente.")

            os.remove(filename)
        except Exception as e:
            print(f"⚠️ Error con la tabla {table}: {e}")


if __name__ == "__main__":
    main()
