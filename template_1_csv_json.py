import json
import pandas as pd
import duckdb
from glom import glom
from loguru import logger
from sqlite_utils import Database
from google.cloud import storage

# === ZONA DE CAMBIOS EN CALIENTE (CONFIGURACIÓN) ===
JSON_PATH = "datos_prueba/clientes_sample.json"
CSV_PATH = "datos_prueba/transacciones_sample.csv"
OUTPUT_CSV = "reporte_conciliacion.csv"
DB_NAME = "migracion_escenario_a.db"
TABLE_NAME = "transacciones_conciliadas"
PK_COLUMN = "tx_id"
MIN_AMOUNT = 1000
REQUIRED_STATUS = "ACTIVE"
BUCKET_NAME = "bucket-de-prueba-icon"
BLOB_DESTINO = "conciliaciones/reporte_conciliacion.csv"
SERVICE_ACCOUNT_FILE = "service_account.json"

logger.add("auditoria_escenario_a.log", level="INFO", encoding="utf-8")

def ejecutar_pipeline():
    logger.info("1. Extrayendo JSON y CSV local...")
    with open(JSON_PATH, mode="r", encoding="utf-8") as f:
        datos_json = json.load(f)

    # >>> CAMBIO EN CALIENTE AQUÍ: Mapeo de campos del JSON anidado con glom
    clientes = []
    for item in datos_json:
        acc_id = str(glom(item, "account_id", default="") or "").strip().upper()
        nombre = str(glom(item, "client.name", default="") or "").strip()
        email = str(glom(item, "client.email", default="") or "").strip().lower()
        estado = str(glom(item, "status", default="") or "").strip().upper()
        if acc_id and nombre and "@" in email:
            clientes.append({"account_id": acc_id, "client_name": nombre, "client_email": email, "status": estado})

    df_clientes = pd.DataFrame(clientes)
    df_txs = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    # >>> CAMBIO EN CALIENTE AQUÍ: Limpieza en Pandas y filtro/cálculo SQL en DuckDB
    df_txs["tx_id"] = df_txs["tx_id"].astype(str).str.strip().str.upper()
    df_txs["account_id"] = df_txs["account_id"].astype(str).str.strip().str.upper()
    df_txs["amount"] = pd.to_numeric(df_txs["amount"].astype(str).str.replace(r"[$,USD\s]", "", regex=True), errors="coerce")

    df_final = duckdb.sql(f"""
        SELECT t.tx_id, t.account_id, c.client_name, c.client_email, c.status,
               t.amount, ROUND(t.amount * 0.05, 2) AS comision, t.tx_date, 'PENDING' AS sync_status
        FROM df_txs t INNER JOIN df_clientes c ON t.account_id = c.account_id
        WHERE t.amount >= {MIN_AMOUNT} AND c.status = '{REQUIRED_STATUS}'
    """).df()

    # >>> CAMBIO EN CALIENTE AQUÍ: Llave primaria (pk) e inserción idempotente en SQLite
    db = Database(DB_NAME)
    tabla = db[TABLE_NAME]
    tabla.insert_all(df_final.to_dict(orient="records"), pk=PK_COLUMN, alter=True, ignore=True)

    pendientes = list(tabla.rows_where("sync_status = ?", ["PENDING"]))
    if pendientes:
        pd.DataFrame(pendientes).to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        try:
            client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
            client.bucket(BUCKET_NAME).blob(BLOB_DESTINO).upload_from_filename(OUTPUT_CSV)
        except Exception as e:
            logger.warning(f"Aviso GCS (continuando en local): {e}")
        for r in pendientes:
            tabla.update(r[PK_COLUMN], {"sync_status": "SYNCED"}, alter=True)
    logger.info(f"Proceso completado. Sincronizados en esta corrida: {len(pendientes)}")

if __name__ == "__main__":
    ejecutar_pipeline()
