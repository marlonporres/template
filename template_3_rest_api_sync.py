import requests
import pandas as pd
import duckdb
from glom import glom
from loguru import logger
from sqlite_utils import Database
from tenacity import retry, stop_after_attempt, wait_exponential

# === ZONA DE CAMBIOS EN CALIENTE (CONFIGURACIÓN) ===
SOURCE_API_URL = "http://127.0.0.1:5000/api/source/records"
WORKBENCH_SYNC_URL = "http://127.0.0.1:5000/api/sync"
API_TOKEN = "token_secreto_123"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
DB_NAME = "migracion_escenario_c.db"
VALID_TABLE = "registros_validos"
REJECTED_TABLE = "registros_rechazados"
PK_COLUMN = "record_id"

logger.add("auditoria_escenario_c.log", level="INFO", encoding="utf-8")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5), reraise=True)
def llamar_api(metodo, url, payload=None):
    resp = requests.request(metodo, url, headers=HEADERS, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()

def ejecutar_pipeline():
    logger.info(f"1. Extrayendo datos desde {SOURCE_API_URL}...")
    respuesta = llamar_api("GET", SOURCE_API_URL)
    items = glom(respuesta, "data", default=respuesta if isinstance(respuesta, list) else [])

    # >>> CAMBIO EN CALIENTE AQUÍ: Mapeo de campos de la API con glom
    filas = []
    for raw in items:
        filas.append({
            "record_id": str(glom(raw, "id", default="") or "").strip().upper(),
            "account_id": str(glom(raw, "account.id", default="") or "").strip().upper(),
            "client_name": str(glom(raw, "account.holder_name", default="") or "").strip(),
            "client_email": str(glom(raw, "account.contact.email", default="") or "").strip().lower(),
            "amount": str(glom(raw, "financials.amount", default="") or "").strip(),
            "status": str(glom(raw, "status", default="") or "").strip().upper(),
        })

    # >>> CAMBIO EN CALIENTE AQUÍ: Limpieza en Pandas y filtro/cálculo SQL en DuckDB
    df = pd.DataFrame(filas)
    df["amount"] = pd.to_numeric(df["amount"].astype(str).str.replace(r"[$,USD\s]", "", regex=True), errors="coerce")
    df_validos = duckdb.sql("SELECT *, 'PENDING' AS sync_status FROM df WHERE amount > 0 AND client_email LIKE '%@%' AND status = 'APPROVED'").df()
    df_rechazados = duckdb.sql("SELECT *, 'DATOS_INVALIDOS' AS rejection_reason FROM df WHERE amount IS NULL OR amount <= 0 OR client_email NOT LIKE '%@%' OR status != 'APPROVED'").df()

    # >>> CAMBIO EN CALIENTE AQUÍ: Llave primaria (pk) e inserción idempotente en SQLite
    db = Database(DB_NAME)
    db[VALID_TABLE].insert_all(df_validos.to_dict(orient="records"), pk=PK_COLUMN, alter=True, ignore=True)
    db[REJECTED_TABLE].insert_all(df_rechazados.to_dict(orient="records"), pk=PK_COLUMN, alter=True, replace=True)

    pendientes = list(db[VALID_TABLE].rows_where("sync_status = ?", ["PENDING"]))
    for reg in pendientes:
        try:
            llamar_api("POST", WORKBENCH_SYNC_URL, payload=reg)
            db[VALID_TABLE].update(reg[PK_COLUMN], {"sync_status": "SYNCED"}, alter=True)
        except Exception as e:
            logger.error(f"Error sincronizando {reg[PK_COLUMN]}: {e}")
    logger.info(f"Proceso completado. Pendientes procesados: {len(pendientes)} | Rechazados: {len(df_rechazados)}")

if __name__ == "__main__":
    ejecutar_pipeline()
