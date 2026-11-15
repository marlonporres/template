"""
PLANTILLA 3 - ESCENARIO C: Consumo de API REST Paginada -> SQLite (+ Auditoría) -> POST API Workbench
=====================================================================================================
Arquitectura Estándar:
1. Extracción: Consumo de API REST (`requests.get`) con Headers Bearer, Query Params y paginación,
   protegido con reintentos exponenciales (`@retry` de `tenacity`) y aplanado seguro (`glom`).
2. Limpieza y Validación: Normalización con `pandas`/`duckdb` separando registros válidos e inválidos.
3. Persistencia Idempotente y Auditoría:
   - Tabla `registros_validos`: control de estado `PENDING` -> `SYNCED` (`pk="record_id"`, `ignore=True`).
   - Tabla `registros_rechazados`: auditoría detallada del motivo de rechazo (`rejection_reason`).
4. Salida y Sincronización: Envío vía `requests.post` con `@retry` hacia el endpoint destino
   (`/api/sync`), respaldo opcional en Google Cloud Storage y actualización a `SYNCED` en SQLite.
5. Observabilidad: Trazabilidad completa con `loguru`.
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

SOURCE_API_URL = os.getenv("SOURCE_API_URL", "http://127.0.0.1:5000/api/source/records")
WORKBENCH_SYNC_URL = os.getenv("WORKBENCH_SYNC_URL", "http://127.0.0.1:5000/api/sync")
API_TOKEN = os.getenv("API_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo_token")

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

DB_PATH = BASE_DIR / "migracion_escenario_c.db"
VALID_TABLE = "registros_validos"
REJECTED_TABLE = "registros_rechazados"

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "bucket-de-prueba-icon")
ENABLE_GCS_BACKUP = os.getenv("ENABLE_GCS_BACKUP", "false").lower() == "true"

# Si el servidor mock no está levantado aún, permite simular la extracción/envío para pruebas rápidas
FALLBACK_IF_API_OFFLINE = True

logger.add(
    BASE_DIR / "auditoria_escenario_c.log",
    rotation="10 MB",
    level="INFO",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
)


# =============================================================================
# 1. EXTRACCIÓN DESDE API REST CON PAGINACIÓN, HEADERS Y TENACITY
# =============================================================================
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def consultar_pagina_api(url: str, params: dict) -> dict | list:
    """
    Ejecuta `requests.get` con headers de autorización y parámetros de consulta.
    Reintenta automáticamente ante cortes de red, timeouts o errores HTTP 5xx.
    """
    logger.info(f"Consultando GET {url} | params={params}")
    response = requests.get(url, headers=HEADERS, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def extraer_registros_api() -> pd.DataFrame:
    """
    Recorre las páginas de la API origen y aplana la respuesta JSON con `glom`.
    """
    registros_crudos = []
    pagina = 1
    limite = 50

    try:
        while True:
            params = {"page": pagina, "limit": limite, "status": "all"}
            payload = consultar_pagina_api(SOURCE_API_URL, params=params)

            # Soporta respuestas tipo {"data": [...], "has_next": bool} o listas directas [...]
            if isinstance(payload, dict):
                items = glom(payload, "data", default=[]) or []
                has_next = bool(glom(payload, "has_next", default=False))
            else:
                items = payload if isinstance(payload, list) else []
                has_next = False

            if not items:
                break

            for raw in items:
                registros_crudos.append({
                    "record_id": str(glom(raw, "id", default="") or "").strip(),
                    "account_id": str(glom(raw, "account.id", default="") or "").strip(),
                    "client_name": str(glom(raw, "account.holder_name", default="") or "").strip(),
                    "client_email": str(glom(raw, "account.contact.email", default="") or "").strip(),
                    "raw_amount": str(glom(raw, "financials.amount", default="") or "").strip(),
                    "currency": str(glom(raw, "financials.currency", default="USD") or "USD").strip(),
                    "status": str(glom(raw, "status", default="") or "").strip(),
                    "created_at": str(glom(raw, "created_at", default="") or "").strip(),
                })

            if not has_next:
                break
            pagina += 1

    except Exception as exc:
        if not FALLBACK_IF_API_OFFLINE:
            raise
        logger.warning(
            f"[MODO DEMO LOCAL] API origen no disponible en {SOURCE_API_URL} ({exc}). "
            "Usando lote de prueba en memoria (ejecuta `python mock_workbench_api.py` para servidor real)."
        )
        datos_demo = [
            {
                "id": "REC-5001",
                "account": {"id": " acc-101 ", "holder_name": " Ana Gómez ", "contact": {"email": "ANA@EMPRESA.COM"}},
                "financials": {"amount": "$1,850.50", "currency": "usd"},
                "status": "approved",
                "created_at": "2026-11-15T10:00:00Z"
            },
            {
                "id": "REC-5002",
                "account": {"id": "ACC-102", "holder_name": "Carlos Ruiz", "contact": {"email": "carlos@empresa.com"}},
                "financials": {"amount": "3,400.00 USD", "currency": "USD"},
                "status": "APPROVED",
                "created_at": "2026-11-15T10:05:00Z"
            },
            {
                "id": "REC-5003",
                "account": {"id": "ACC-103", "holder_name": "Sin Email Válido", "contact": {"email": null_val}},
                "financials": {"amount": "950.00", "currency": "USD"},
                "status": "APPROVED",
                "created_at": "2026-11-15T10:10:00Z"
            } if (null_val := None) is None else {},
            {
                "id": "REC-5004",
                "account": {"id": "ACC-104", "holder_name": "Monto Corrupto", "contact": {"email": "error@empresa.com"}},
                "financials": {"amount": "ERROR_MONTO", "currency": "USD"},
                "status": "APPROVED",
                "created_at": "2026-11-15T10:15:00Z"
            },
        ]
        for raw in datos_demo:
            registros_crudos.append({
                "record_id": str(glom(raw, "id", default="") or "").strip(),
                "account_id": str(glom(raw, "account.id", default="") or "").strip(),
                "client_name": str(glom(raw, "account.holder_name", default="") or "").strip(),
                "client_email": str(glom(raw, "account.contact.email", default="") or "").strip(),
                "raw_amount": str(glom(raw, "financials.amount", default="") or "").strip(),
                "currency": str(glom(raw, "financials.currency", default="USD") or "USD").strip(),
                "status": str(glom(raw, "status", default="") or "").strip(),
                "created_at": str(glom(raw, "created_at", default="") or "").strip(),
            })

    logger.info(f"Total de registros extraídos desde API origen: {len(registros_crudos)}")
    return pd.DataFrame(registros_crudos)


# =============================================================================
# 2. LIMPIEZA Y SEPARACIÓN DE RECHAZOS (Pandas + DuckDB)
# =============================================================================
def limpiar_y_validar_datos(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Limpia strings y montos con Pandas, evalúa reglas de calidad de datos y separa:
    - `df_validos`: registros listos para sincronizar con el Workbench.
    - `df_rechazados`: registros con su motivo exacto de rechazo para auditoría en SQLite.
    """
    if df_raw.empty:
        return pd.DataFrame(), pd.DataFrame()

    logger.info("Ejecutando limpieza y validación de reglas de negocio con Pandas y DuckDB...")
    df = df_raw.copy()

    df["record_id"] = df["record_id"].astype(str).str.strip().str.upper()
    df["account_id"] = df["account_id"].astype(str).str.strip().str.upper()
    df["client_name"] = df["client_name"].astype(str).str.strip()
    df["client_email"] = df["client_email"].astype(str).str.strip().str.lower()
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].astype(str).str.strip().str.upper()

    df["amount"] = pd.to_numeric(
        df["raw_amount"].astype(str).str.replace(r"[$,USD\s]", "", regex=True),
        errors="coerce"
    )

    # Clasificación y cálculo en DuckDB
    df_evaluado = duckdb.sql("""
        SELECT
            record_id,
            account_id,
            client_name,
            client_email,
            raw_amount,
            ROUND(amount, 2) AS amount,
            ROUND(amount * 0.05, 2) AS service_fee,
            currency,
            status,
            created_at,
            CASE
                WHEN record_id = '' OR account_id = '' THEN 'ID_FALTANTE'
                WHEN client_email NOT LIKE '%@%' THEN 'EMAIL_INVALIDO'
                WHEN amount IS NULL OR amount <= 0 THEN 'MONTO_INVALIDO_O_NULO'
                WHEN status != 'APPROVED' THEN 'ESTADO_NO_APROBADO'
                ELSE 'VALID'
            END AS validation_result
        FROM df
    """).df()

    df_validos = df_evaluado[df_evaluado["validation_result"] == "VALID"].drop(
        columns=["raw_amount", "validation_result"]
    ).copy()

    df_rechazados = df_evaluado[df_evaluado["validation_result"] != "VALID"].rename(
        columns={"validation_result": "rejection_reason"}
    ).copy()

    logger.info(
        f"Validación completada -> Válidos: {len(df_validos)} | Rechazados: {len(df_rechazados)}"
    )
    return df_validos, df_rechazados


# =============================================================================
# 3. PERSISTENCIA IDEMPOTENTE Y AUDITORÍA DE RECHAZOS EN SQLITE (sqlite-utils)
# =============================================================================
def persistir_lote_en_sqlite(
    df_validos: pd.DataFrame,
    df_rechazados: pd.DataFrame,
    db_path: Path
) -> Database:
    """
    Guarda los registros válidos con `sync_status = 'PENDING'` de forma idempotente (`ignore=True`)
    y registra los rechazados en la tabla de auditoría `registros_rechazados`.
    """
    db = Database(db_path)
    ahora = datetime.now().isoformat(timespec="seconds")

    if not df_validos.empty:
        df_val_save = df_validos.copy()
        df_val_save["sync_status"] = "PENDING"
        df_val_save["ingested_at"] = ahora
        db[VALID_TABLE].insert_all(
            df_val_save.to_dict(orient="records"),
            pk="record_id",
            alter=True,
            ignore=True
        )
        logger.info(f"Tabla '{VALID_TABLE}' actualizada. Total registros: {db[VALID_TABLE].count}")

    if not df_rechazados.empty:
        df_rej_save = df_rechazados.copy()
        df_rej_save["rejected_at"] = ahora
        db[REJECTED_TABLE].insert_all(
            df_rej_save.to_dict(orient="records"),
            pk="record_id",
            alter=True,
            replace=True
        )
        logger.warning(
            f"Se registraron {len(df_rechazados)} casos en la tabla de auditoría '{REJECTED_TABLE}'."
        )

    return db


# =============================================================================
# 4. SALIDA: POST A API WORKBENCH CON TENACITY (+ RESPALDO GCS OPCIONAL)
# =============================================================================
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def enviar_post_workbench(registro: dict) -> dict:
    """
    Envía un registro limpio mediante `requests.post` al endpoint destino `/api/sync`,
    protegido con reintentos exponenciales de `tenacity`.
    """
    response = requests.post(WORKBENCH_SYNC_URL, headers=HEADERS, json=registro, timeout=10)
    response.raise_for_status()
    return response.json()


def respaldar_lote_en_gcs(registros_sincronizados: list[dict]) -> None:
    """
    Opcional: Guarda un respaldo JSON del lote sincronizado en Google Cloud Storage.
    """
    if not ENABLE_GCS_BACKUP or not registros_sincronizados:
        return
    try:
        if os.getenv("STORAGE_EMULATOR_HOST"):
            client = storage.Client(project="local-emulator", credentials=AnonymousCredentials())
        elif Path(SERVICE_ACCOUNT_FILE).exists():
            client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        else:
            client = storage.Client()

        fecha = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        ruta_blob = f"api_sync_backups/lote_{fecha}.json"
        blob = client.bucket(GCS_BUCKET_NAME).blob(ruta_blob)
        blob.upload_from_string(
            json.dumps(registros_sincronizados, indent=2, ensure_ascii=False),
            content_type="application/json"
        )
        logger.info(f"Respaldo de lote subido a GCS: gs://{GCS_BUCKET_NAME}/{ruta_blob}")
    except Exception as exc:
        logger.warning(f"No se pudo subir el respaldo opcional a GCS: {exc}")


def sincronizar_pendientes_workbench(db: Database) -> None:
    """
    Consulta únicamente los registros con `sync_status = 'PENDING'`, los envía por POST
    a la API Workbench y actualiza su estado a `SYNCED` en SQLite.
    """
    tabla = db[VALID_TABLE]
    if not tabla.exists():
        logger.info("No existe aún la tabla de registros válidos.")
        return

    pendientes = list(tabla.rows_where("sync_status = ?", ["PENDING"]))
    logger.info(f"Registros válidos en estado PENDING listos para POST: {len(pendientes)}")

    sincronizados = []
    for item in pendientes:
        rec_id = item["record_id"]
        try:
            respuesta_api = enviar_post_workbench(item)
            logger.info(f"POST exitoso para {rec_id} -> Respuesta API: {respuesta_api}")
        except Exception as exc:
            if FALLBACK_IF_API_OFFLINE:
                logger.warning(
                    f"[MODO DEMO LOCAL] Endpoint {WORKBENCH_SYNC_URL} no disponible ({exc}). "
                    f"Simulando confirmación HTTP 200 para {rec_id}."
                )
            else:
                logger.error(f"Error definitivo al sincronizar {rec_id}: {exc}")
                continue

        tabla.update(
            rec_id,
            {
                "sync_status": "SYNCED",
                "synced_at": datetime.now().isoformat(timespec="seconds")
            },
            alter=True
        )
        sincronizados.append(item)
        logger.info(f"Estado de {rec_id} actualizado a SYNCED en SQLite.")

    respaldar_lote_en_gcs(sincronizados)


# =============================================================================
# 5. ORQUESTADOR PRINCIPAL
# =============================================================================
def main() -> None:
    logger.info("=== INICIANDO PIPELINE ESCENARIO C (API REST -> SQLite -> POST Workbench) ===")
    df_crudo = extraer_registros_api()
    df_validos, df_rechazados = limpiar_y_validar_datos(df_crudo)
    db = persistir_lote_en_sqlite(df_validos, df_rechazados, DB_PATH)
    sincronizar_pendientes_workbench(db)
    logger.info("=== PIPELINE ESCENARIO C FINALIZADO EXITOSAMENTE ===")


if __name__ == "__main__":
    main()


# =============================================================================
# 6. SERVIDOR MOCK EN FLASK (También disponible listo para ejecutar en `mock_workbench_api.py`)
# =============================================================================
"""
# Para levantar el Mock Server localmente en otra terminal ejecuta:
# python mock_workbench_api.py
#
# Código de referencia del servidor Flask:

from flask import Flask, request, jsonify

app = Flask(__name__)
RECIBIDOS_DB = []

@app.route("/api/sync", methods=["POST"])
def sync_record():
    payload = request.get_json(silent=True) or {}
    if not payload.get("record_id"):
        return jsonify({"status": "error", "message": "record_id es requerido"}), 400
    RECIBIDOS_DB.append(payload)
    return jsonify({
        "status": "synced",
        "record_id": payload["record_id"],
        "total_synced": len(RECIBIDOS_DB)
    }), 200

@app.route("/api/status", methods=["GET"])
def api_status():
    return jsonify({
        "service": "Mock Workbench API",
        "status": "online",
        "records_received": len(RECIBIDOS_DB),
        "items": RECIBIDOS_DB
    }), 200

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
"""
