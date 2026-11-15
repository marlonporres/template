"""
PLANTILLA 1 - ESCENARIO A: Ingesta y Cruce de Archivos Locales (CSV + JSON Anidado)
====================================================================================
Arquitectura Estándar:
1. Extracción: Lectura segura de JSON anidado con `glom` y CSV local con `utf-8-sig`.
2. Limpieza y Cruce: Normalización con `pandas` + `INNER JOIN` SQL con `duckdb`.
3. Persistencia Idempotente: Control de estado (`PENDING` -> `SYNCED`) en SQLite con `sqlite-utils`.
4. Salida y Sincronización: Exportación CSV/JSON + Subida a Google Cloud Storage (GCS) + POST con `tenacity`.
5. Observabilidad: Trazabilidad completa en consola y archivo rotativo con `loguru`.
"""

import os
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import duckdb
import requests
from glom import glom
from loguru import logger
from sqlite_utils import Database
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials


# =============================================================================
# 0. CONFIGURACIÓN GLOBAL Y LOGGING
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
JSON_INPUT_PATH = BASE_DIR / "datos_prueba" / "clientes_sample.json"
CSV_INPUT_PATH = BASE_DIR / "datos_prueba" / "transacciones_sample.csv"

OUTPUT_CSV_PATH = BASE_DIR / "reporte_conciliacion.csv"
OUTPUT_JSON_PATH = BASE_DIR / "reporte_conciliacion.json"

DB_PATH = BASE_DIR / "migracion_escenario_a.db"
TABLE_NAME = "transacciones_conciliadas"

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "bucket-de-prueba-icon")
WORKBENCH_API_URL = os.getenv("WORKBENCH_API_URL", "")  # Opcional: URL de API destino si aplica

# Si no hay credenciales ni emulador GCS activo, permite probar el flujo completo localmente
ALLOW_LOCAL_GCS_SIMULATION = True

logger.add(
    BASE_DIR / "auditoria_escenario_a.log",
    rotation="10 MB",
    level="INFO",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
)


# =============================================================================
# 1. EXTRACCIÓN Y APLANADO SEGURO (JSON con glom + CSV con utf-8-sig)
# =============================================================================
def extraer_clientes_json(ruta_json: Path) -> pd.DataFrame:
    """
    Lee un archivo JSON con estructuras anidadas y posibles valores nulos (`null`).
    Utiliza `glom` para extraer campos de forma segura sin errores de `KeyError` o `TypeError`.
    """
    logger.info(f"Leyendo archivo JSON de clientes: {ruta_json}")
    with open(ruta_json, mode="r", encoding="utf-8") as f:
        datos_crudos = json.load(f)

    clientes_limpios = []
    for idx, item in enumerate(datos_crudos):
        # glom navega diccionarios anidados; `or ""` protege cuando el valor en JSON es `null` (None)
        acc_id = str(glom(item, "account_id", default="") or "").strip().upper()
        nombre = str(glom(item, "client.name", default="") or "").strip()
        email = str(glom(item, "client.email", default="") or "").strip().lower()
        segmento = str(glom(item, "client.segment", default="General") or "General").strip().upper()
        estado = str(glom(item, "status", default="") or "").strip().upper()

        # Validación de reglas de negocio básicas en extracción de clientes
        if acc_id and nombre and "@" in email:
            clientes_limpios.append({
                "account_id": acc_id,
                "client_name": nombre,
                "client_email": email,
                "segment": segmento,
                "status": estado
            })
        else:
            logger.warning(
                f"Cliente omitido en índice {idx} por datos incompletos o email inválido: "
                f"account_id='{acc_id}', name='{nombre}', email='{email}'"
            )

    logger.info(f"Clientes válidos extraídos con glom: {len(clientes_limpios)} de {len(datos_crudos)}")
    return pd.DataFrame(clientes_limpios)


def extraer_transacciones_csv(ruta_csv: Path) -> pd.DataFrame:
    """
    Lee el archivo CSV local usando `encoding='utf-8-sig'` para eliminar posibles BOM de Excel.
    """
    logger.info(f"Leyendo archivo CSV de transacciones: {ruta_csv}")
    df_txs = pd.read_csv(ruta_csv, encoding="utf-8-sig")
    logger.info(f"Filas leídas desde CSV: {len(df_txs)}")
    return df_txs


# =============================================================================
# 2. LIMPIEZA (Pandas) Y CRUCE RELACIONAL (DuckDB)
# =============================================================================
def limpiar_y_cruzar_datos(df_clientes: pd.DataFrame, df_txs: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza cadenas de texto, convierte montos monetarios sucios a float y ejecuta
    un INNER JOIN relacional en memoria con DuckDB calculando columnas adicionales.
    """
    logger.info("Iniciando limpieza de transacciones con Pandas...")
    df_txs = df_txs.copy()

    # Limpieza de strings: quitar espacios laterales y estandarizar mayúsculas
    df_txs["tx_id"] = df_txs["tx_id"].astype(str).str.strip().str.upper()
    df_txs["account_id"] = df_txs["account_id"].astype(str).str.strip().str.upper()
    df_txs["tx_date"] = df_txs["tx_date"].astype(str).str.strip()

    # Limpieza de columnas de dinero: quitar '$', ',', 'USD' y espacios; textos inválidos pasan a NaN
    df_txs["amount"] = pd.to_numeric(
        df_txs["amount"].astype(str).str.replace(r"[$,USD\s]", "", regex=True),
        errors="coerce"
    )

    # Cruce SQL directo sobre los DataFrames de Pandas usando DuckDB
    logger.info("Ejecutando cruce relacional (INNER JOIN) y reglas de negocio con DuckDB...")
    query_sql = """
        SELECT
            t.tx_id,
            t.account_id,
            c.client_name,
            c.client_email,
            c.segment,
            c.status AS account_status,
            ROUND(t.amount, 2) AS amount,
            ROUND(t.amount * 0.05, 2) AS fee_amount,
            ROUND(t.amount * 0.95, 2) AS net_amount,
            t.tx_date
        FROM df_txs t
        INNER JOIN df_clientes c
            ON t.account_id = c.account_id
        WHERE t.amount IS NOT NULL
          AND t.amount >= 1000.00
          AND c.status = 'ACTIVE'
        ORDER BY t.tx_id ASC
    """
    df_resultado = duckdb.sql(query_sql).df()
    logger.info(f"Registros resultantes tras cruce en DuckDB: {len(df_resultado)}")
    return df_resultado


# =============================================================================
# 3. PERSISTENCIA IDEMPOTENTE EN SQLITE (sqlite-utils)
# =============================================================================
def guardar_en_sqlite_idempotente(df_limpio: pd.DataFrame, db_path: Path) -> Database:
    """
    Inserta los registros en SQLite usando `pk='tx_id'` e `ignore=True`.
    Si el script se ejecuta múltiples veces, NO duplica registros ni sobrescribe
    aquellos que ya cambiaron su estado a `SYNCED`.
    """
    db = Database(db_path)
    tabla = db[TABLE_NAME]

    if df_limpio.empty:
        logger.warning("El DataFrame limpio está vacío; no hay nuevos registros para insertar en SQLite.")
        return db

    df_to_save = df_limpio.copy()
    df_to_save["sync_status"] = "PENDING"
    df_to_save["created_at"] = datetime.now().isoformat(timespec="seconds")

    registros = df_to_save.to_dict(orient="records")

    # Inserción idempotente: ignore=True salta los tx_id que ya existen en la tabla
    tabla.insert_all(
        registros,
        pk="tx_id",
        alter=True,
        ignore=True
    )
    logger.info(f"Persistencia en SQLite completada. Total histórico en '{TABLE_NAME}': {tabla.count}")
    return db


# =============================================================================
# 4. SALIDA: EXPORTACIÓN LOCAL + SUBIDA A GOOGLE CLOUD STORAGE (+ RETRY)
# =============================================================================
def obtener_cliente_gcs() -> storage.Client:
    """
    Inicializa el cliente de Google Cloud Storage soportando:
    1) Emulador local (`STORAGE_EMULATOR_HOST`).
    2) Archivo `service_account.json`.
    3) Application Default Credentials (ADC).
    """
    if os.getenv("STORAGE_EMULATOR_HOST"):
        logger.info(f"Usando emulador GCS en {os.getenv('STORAGE_EMULATOR_HOST')}")
        return storage.Client(project="local-emulator", credentials=AnonymousCredentials())

    if Path(SERVICE_ACCOUNT_FILE).exists():
        logger.info(f"Autenticando en GCS con archivo de credenciales: {SERVICE_ACCOUNT_FILE}")
        return storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

    return storage.Client()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def subir_archivo_a_gcs(ruta_local: Path, ruta_destino_blob: str, content_type: str) -> str:
    """
    Sube un archivo local a un bucket de Google Cloud Storage con reintentos automáticos (`tenacity`).
    """
    client = obtener_cliente_gcs()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(ruta_destino_blob)
    blob.upload_from_filename(str(ruta_local), content_type=content_type)
    uri = f"gs://{GCS_BUCKET_NAME}/{ruta_destino_blob}"
    logger.info(f"Archivo subido exitosamente a GCS: {uri}")
    return uri


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def enviar_registro_api(payload: dict) -> dict:
    """
    Envía un registro mediante POST a una API REST (si `WORKBENCH_API_URL` está configurada)
    protegido con backoff exponencial de `tenacity`.
    """
    resp = requests.post(WORKBENCH_API_URL, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def exportar_y_sincronizar(db: Database) -> None:
    """
    Consulta los registros en estado `PENDING`, exporta el reporte consolidado a CSV/JSON,
    lo sube a Google Cloud Storage y actualiza cada registro a `SYNCED` en SQLite.
    """
    tabla = db[TABLE_NAME]
    pendientes = list(tabla.rows_where("sync_status = ?", ["PENDING"]))
    logger.info(f"Registros con estado PENDING por sincronizar: {len(pendientes)}")

    if not pendientes:
        logger.info("No hay registros pendientes de sincronización. Proceso idempotente finalizado.")
        return

    # 1. Exportar reporte consolidado a CSV y JSON
    df_pendientes = pd.DataFrame(pendientes)
    df_pendientes.to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8")
    df_pendientes.to_json(OUTPUT_JSON_PATH, orient="records", indent=2, force_ascii=False)
    logger.info(f"Reportes locales generados: '{OUTPUT_CSV_PATH.name}' y '{OUTPUT_JSON_PATH.name}'")

    # 2. Subir reportes a Google Cloud Storage
    fecha_lote = datetime.now().strftime("%Y-%m-%d")
    blob_csv = f"conciliaciones/{fecha_lote}/{OUTPUT_CSV_PATH.name}"
    blob_json = f"conciliaciones/{fecha_lote}/{OUTPUT_JSON_PATH.name}"

    try:
        subir_archivo_a_gcs(OUTPUT_CSV_PATH, blob_csv, content_type="text/csv")
        subir_archivo_a_gcs(OUTPUT_JSON_PATH, blob_json, content_type="application/json")
    except Exception as exc:
        if ALLOW_LOCAL_GCS_SIMULATION:
            logger.warning(
                f"[SIMULACIÓN LOCAL GCS] No se detectó conexión activa a GCS ({exc}). "
                f"Se simula subida a gs://{GCS_BUCKET_NAME}/{blob_json} para pruebas locales."
            )
        else:
            logger.error(f"Error crítico subiendo reporte a GCS: {exc}")
            raise

    # 3. Actualizar estado individual en SQLite a SYNCED
    for reg in pendientes:
        tx_id = reg["tx_id"]
        if WORKBENCH_API_URL:
            enviar_registro_api(reg)

        tabla.update(
            tx_id,
            {
                "sync_status": "SYNCED",
                "synced_at": datetime.now().isoformat(timespec="seconds")
            },
            alter=True
        )
        logger.info(f"Transacción {tx_id} marcada como SYNCED en SQLite.")


# =============================================================================
# 5. ORQUESTADOR PRINCIPAL
# =============================================================================
def main() -> None:
    logger.info("=== INICIANDO PIPELINE ESCENARIO A (CSV + JSON -> DuckDB -> SQLite -> GCS) ===")
    df_clientes = extraer_clientes_json(JSON_INPUT_PATH)
    df_txs = extraer_transacciones_csv(CSV_INPUT_PATH)

    df_conciliado = limpiar_y_cruzar_datos(df_clientes, df_txs)
    db = guardar_en_sqlite_idempotente(df_conciliado, DB_PATH)
    exportar_y_sincronizar(db)
    logger.info("=== PIPELINE ESCENARIO A FINALIZADO EXITOSAMENTE ===")


if __name__ == "__main__":
    main()
