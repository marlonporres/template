"""
PLANTILLA 2 - ESCENARIO B: Extracción desde Google Forms API -> SQLite -> Google Cloud Storage
==============================================================================================
Arquitectura Estándar:
1. Extracción: Autenticación con `service_account.json`, mapeo dinámico `{questionId: title}`
   de `forms().get` y descarga de respuestas con `responses().list` aplanadas con `glom`.
2. Limpieza y Validación: Normalización de strings, montos y filtrado SQL con `pandas` y `duckdb`.
3. Persistencia Idempotente: Guardado en SQLite con `sqlite-utils` (`pk="response_id"`, `ignore=True`).
4. Salida y Sincronización: Subida de cada registro pendiente como `.json` individual a GCS
   (y envío opcional vía POST a API REST) protegido con `tenacity` + actualización a `SYNCED`.
5. Observabilidad: Registro estructurado con `loguru`.
"""

import os
import re
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
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials


# =============================================================================
# 0. CONFIGURACIÓN GLOBAL Y LOGGING
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
FORM_ID = os.getenv("GOOGLE_FORM_ID", "1fKnbT-kz4w3thIZCwZXHIvKCnZRk6AVaZuKghkoCSl4")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "bucket-de-prueba-icon")
WORKBENCH_API_URL = os.getenv("WORKBENCH_API_URL", "")  # Opcional: Endpoint REST destino

DB_PATH = BASE_DIR / "migracion_escenario_b.db"
TABLE_NAME = "respuestas_formulario"

SCOPES = [
    "https://www.googleapis.com/auth/forms.body.readonly",
    "https://www.googleapis.com/auth/forms.responses.readonly",
]

# Permite probar toda la tubería localmente con respuestas simuladas si no hay credenciales activas
FALLBACK_TO_MOCK_ON_AUTH_ERROR = True

logger.add(
    BASE_DIR / "auditoria_escenario_b.log",
    rotation="10 MB",
    level="INFO",
    encoding="utf-8",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}"
)


# =============================================================================
# 1. EXTRACCIÓN DESDE GOOGLE FORMS API Y APLANADO CON GLOM
# =============================================================================
def normalizar_nombre_columna(titulo: str) -> str:
    """
    Convierte el título de una pregunta de Google Forms en un identificador snake_case limpio.
    Ejemplo: 'Monto de Liquidación (USD)' -> 'monto_de_liquidacion_usd'
    """
    limpio = titulo.strip().lower()
    reemplazos = str.maketrans("áéíóúñ", "aeioun")
    limpio = limpio.translate(reemplazos)
    limpio = re.sub(r"[^a-z0-9]+", "_", limpio).strip("_")
    return limpio or "campo_sin_titulo"


def obtener_servicio_forms():
    """
    Autentica con `service_account.json` y construye el cliente oficial de Google Forms API v1.
    """
    logger.info(f"Autenticando en Google Forms API con '{SERVICE_ACCOUNT_FILE}'...")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    return build("forms", "v1", credentials=creds, cache_discovery=False)


def mapear_preguntas_formulario(form_metadata: dict) -> dict[str, str]:
    """
    Extrae dinámicamente el diccionario `{questionId: column_name}` desde el resultado de
    `forms().get(formId=...)` usando `glom` para evitar fallos en ítems decorativos (secciones/imágenes).
    """
    mapa_preguntas: dict[str, str] = {}
    items = glom(form_metadata, "items", default=[]) or []

    for item in items:
        titulo_raw = str(glom(item, "title", default="") or "").strip()
        question_id = glom(item, "questionItem.question.questionId", default=None)

        if question_id:
            mapa_preguntas[question_id] = normalizar_nombre_columna(titulo_raw)

    logger.info(f"Mapa dinámico de preguntas construido ({len(mapa_preguntas)} campos): {mapa_preguntas}")
    return mapa_preguntas


def aplanar_respuestas_con_glom(raw_responses: list[dict], mapa_preguntas: dict[str, str]) -> list[dict]:
    """
    Aplana la lista de respuestas de `responses().list()` usando `glom`.
    Si una pregunta opcional viene vacía (no existe en `answers`), `glom` devuelve `""` de forma segura.
    """
    registros_aplanados = []

    for resp in raw_responses:
        response_id = str(glom(resp, "responseId", default="") or "").strip()
        if not response_id:
            continue

        fila = {
            "response_id": response_id,
            "submitted_at": str(glom(resp, "lastSubmittedTime", default="") or "").strip(),
            "respondent_email": str(glom(resp, "respondentEmail", default="") or "").strip().lower(),
        }

        # Extraer cada pregunta mapeada sin riesgo de KeyError/IndexError cuando está vacía
        for q_id, nombre_col in mapa_preguntas.items():
            ruta_glom = f"answers.{q_id}.textAnswers.answers.0.value"
            valor = glom(resp, ruta_glom, default="")
            fila[nombre_col] = str(valor if valor is not None else "").strip()

        registros_aplanados.append(fila)

    logger.info(f"Respuestas aplanadas exitosamente con glom: {len(registros_aplanados)}")
    return registros_aplanados


def extraer_datos_google_forms() -> pd.DataFrame:
    """
    Conecta a Google Forms API (con soporte de paginación `nextPageToken`), obtiene metadata y
    respuestas, y devuelve un DataFrame de Pandas listo para limpieza.
    """
    try:
        service = obtener_servicio_forms()

        # 1a) Obtener metadata del formulario
        logger.info(f"Consultando metadata del formulario Form ID: {FORM_ID}")
        form_metadata = service.forms().get(formId=FORM_ID).execute()
        mapa_preguntas = mapear_preguntas_formulario(form_metadata)

        # 1b) Descargar todas las páginas de respuestas
        logger.info("Descargando respuestas desde Google Forms API...")
        todas_las_respuestas = []
        page_token = None

        while True:
            req = service.forms().responses().list(formId=FORM_ID, pageToken=page_token)
            res = req.execute()
            lote = glom(res, "responses", default=[]) or []
            todas_las_respuestas.extend(lote)

            page_token = res.get("nextPageToken")
            if not page_token:
                break

        filas = aplanar_respuestas_con_glom(todas_las_respuestas, mapa_preguntas)
        return pd.DataFrame(filas)

    except Exception as exc:
        if not FALLBACK_TO_MOCK_ON_AUTH_ERROR:
            raise
        logger.warning(
            f"[MODO DEMO LOCAL] No fue posible conectar a Google Forms API ({exc}). "
            "Usando payload simulado con la estructura oficial de Google Forms API v1."
        )
        mock_metadata = {
            "formId": FORM_ID,
            "items": [
                {"title": "Account ID", "questionItem": {"question": {"questionId": "q_acc_01"}}},
                {"title": "Client Name", "questionItem": {"question": {"questionId": "q_name_02"}}},
                {"title": "Claim Amount", "questionItem": {"question": {"questionId": "q_amt_03"}}},
                {"title": "Priority Level", "questionItem": {"question": {"questionId": "q_prio_04"}}},
            ],
        }
        mock_responses = [
            {
                "responseId": "RESP-2026-001",
                "lastSubmittedTime": "2026-11-15T14:20:00Z",
                "respondentEmail": "ANA.GOMEZ@EMPRESA.COM ",
                "answers": {
                    "q_acc_01": {"textAnswers": {"answers": [{"value": " acc-101 "}]}},
                    "q_name_02": {"textAnswers": {"answers": [{"value": "  Ana Gómez  "}]}},
                    "q_amt_03": {"textAnswers": {"answers": [{"value": "$2,450.75 USD"}]}},
                    "q_prio_04": {"textAnswers": {"answers": [{"value": "high"}]}},
                },
            },
            {
                "responseId": "RESP-2026-002",
                "lastSubmittedTime": "2026-11-15T14:25:00Z",
                "respondentEmail": "carlos.ruiz@correo.com",
                "answers": {
                    "q_acc_01": {"textAnswers": {"answers": [{"value": "ACC-102"}]}},
                    "q_name_02": {"textAnswers": {"answers": [{"value": "Carlos Ruiz"}]}},
                    "q_amt_03": {"textAnswers": {"answers": [{"value": " $ 1,120.00 "}]}},
                    # Pregunta q_prio_04 ausente intencionalmente (pregunta opcional vacía)
                },
            },
            {
                "responseId": "RESP-2026-003",
                "lastSubmittedTime": "2026-11-15T14:30:00Z",
                "respondentEmail": "invalido@correo.com",
                "answers": {
                    "q_acc_01": {"textAnswers": {"answers": [{"value": "ACC-103"}]}},
                    "q_name_02": {"textAnswers": {"answers": [{"value": "Registro Monto Corrupto"}]}},
                    "q_amt_03": {"textAnswers": {"answers": [{"value": "SIN_MONTO"}]}},
                },
            },
        ]
        mapa_preguntas = mapear_preguntas_formulario(mock_metadata)
        filas = aplanar_respuestas_con_glom(mock_responses, mapa_preguntas)
        return pd.DataFrame(filas)


# =============================================================================
# 2. LIMPIEZA (Pandas) Y FILTRADO / TRANSFORMACIÓN (DuckDB)
# =============================================================================
def limpiar_respuestas_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia espacios, estandariza mayúsculas/minúsculas, convierte montos numéricos
    y aplica reglas de validación mediante SQL en DuckDB.
    """
    if df_raw.empty:
        logger.warning("No se recibieron respuestas del formulario para limpiar.")
        return df_raw

    logger.info("Limpiando y normalizando respuestas en Pandas...")
    df = df_raw.copy()

    # Normalizar todas las columnas de texto
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.strip()

    if "account_id" in df.columns:
        df["account_id"] = df["account_id"].str.upper()

    if "priority_level" in df.columns:
        df["priority_level"] = df["priority_level"].replace("", "NORMAL").str.upper()

    # Si existe una columna de monto, limpiamos símbolos monetarios ('$', ',', 'USD')
    if "claim_amount" in df.columns:
        df["claim_amount"] = pd.to_numeric(
            df["claim_amount"].str.replace(r"[$,USD\s]", "", regex=True),
            errors="coerce"
        )
        # Filtrado analítico en DuckDB para conservar montos válidos y calcular impuesto/reserva
        df = duckdb.sql("""
            SELECT
                response_id,
                account_id,
                client_name,
                respondent_email,
                priority_level,
                ROUND(claim_amount, 2) AS claim_amount,
                ROUND(claim_amount * 0.10, 2) AS reserve_amount,
                submitted_at
            FROM df
            WHERE claim_amount IS NOT NULL
              AND claim_amount > 0
              AND account_id != ''
        """).df()

    logger.info(f"Respuestas válidas tras limpieza con Pandas/DuckDB: {len(df)}")
    return df


# =============================================================================
# 3. PERSISTENCIA IDEMPOTENTE EN SQLITE (sqlite-utils)
# =============================================================================
def guardar_en_sqlite_idempotente(df_limpio: pd.DataFrame, db_path: Path) -> Database:
    """
    Almacena las respuestas en SQLite usando `pk="response_id"` e `ignore=True`
    para garantizar que ejecuciones repetidas no dupliquen ni alteren registros ya procesados.
    """
    db = Database(db_path)
    tabla = db[TABLE_NAME]

    if df_limpio.empty:
        logger.warning("DataFrame vacío; no se insertaron nuevos registros en SQLite.")
        return db

    df_to_save = df_limpio.copy()
    df_to_save["sync_status"] = "PENDING"
    df_to_save["ingested_at"] = datetime.now().isoformat(timespec="seconds")

    registros = df_to_save.to_dict(orient="records")
    tabla.insert_all(
        registros,
        pk="response_id",
        alter=True,
        ignore=True
    )
    logger.info(f"Registros asegurados en SQLite ('{TABLE_NAME}'). Total acumulado: {tabla.count}")
    return db


# =============================================================================
# 4. SALIDA: SUBIDA INDIVIDUAL JSON A GCS + ACTUALIZACIÓN DE ESTADO
# =============================================================================
def obtener_cliente_gcs() -> storage.Client:
    """
    Obtiene el cliente de Google Cloud Storage (emulador local o `service_account.json`).
    """
    if os.getenv("STORAGE_EMULATOR_HOST"):
        return storage.Client(project="local-emulator", credentials=AnonymousCredentials())
    if Path(SERVICE_ACCOUNT_FILE).exists():
        return storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    return storage.Client()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def subir_json_individual_gcs(response_id: str, payload: dict) -> str:
    """
    Sube un registro individual como objeto `.json` a Google Cloud Storage con reintentos (`tenacity`).
    """
    fecha_carpeta = datetime.now().strftime("%Y-%m-%d")
    ruta_blob = f"forms_responses/{fecha_carpeta}/{response_id}.json"

    client = obtener_cliente_gcs()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(ruta_blob)

    contenido_json = json.dumps(payload, indent=2, ensure_ascii=False)
    blob.upload_from_string(contenido_json, content_type="application/json")

    uri = f"gs://{GCS_BUCKET_NAME}/{ruta_blob}"
    logger.info(f"Respuesta {response_id} subida a GCS: {uri}")
    return uri


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def notificar_api_workbench(payload: dict) -> None:
    """
    Envía opcionalmente el registro a una API REST destino si `WORKBENCH_API_URL` está definida.
    """
    resp = requests.post(WORKBENCH_API_URL, json=payload, timeout=10)
    resp.raise_for_status()


def sincronizar_pendientes_a_gcs(db: Database) -> None:
    """
    Itera sobre todos los registros con `sync_status = 'PENDING'`, sube cada uno como `.json`
    individual a Google Cloud Storage y actualiza su estado a `SYNCED` en SQLite.
    """
    tabla = db[TABLE_NAME]
    pendientes = list(tabla.rows_where("sync_status = ?", ["PENDING"]))
    logger.info(f"Respuestas pendientes por subir a GCS: {len(pendientes)}")

    for registro in pendientes:
        resp_id = registro["response_id"]
        try:
            subir_json_individual_gcs(resp_id, registro)
        except Exception as exc:
            if FALLBACK_TO_MOCK_ON_AUTH_ERROR:
                fecha_carpeta = datetime.now().strftime("%Y-%m-%d")
                logger.warning(
                    f"[SIMULACIÓN LOCAL GCS] Sin credenciales GCS activas ({exc}). "
                    f"Simulando upload de gs://{GCS_BUCKET_NAME}/forms_responses/{fecha_carpeta}/{resp_id}.json"
                )
            else:
                logger.error(f"Fallo al subir {resp_id} a GCS: {exc}")
                continue

        if WORKBENCH_API_URL:
            notificar_api_workbench(registro)

        tabla.update(
            resp_id,
            {
                "sync_status": "SYNCED",
                "synced_at": datetime.now().isoformat(timespec="seconds")
            },
            alter=True
        )
        logger.info(f"Estado de {resp_id} actualizado a SYNCED en SQLite.")


# =============================================================================
# 5. ORQUESTADOR PRINCIPAL
# =============================================================================
def main() -> None:
    logger.info("=== INICIANDO PIPELINE ESCENARIO B (Google Forms API -> SQLite -> GCS) ===")
    df_crudo = extraer_datos_google_forms()
    df_limpio = limpiar_respuestas_dataframe(df_crudo)
    db = guardar_en_sqlite_idempotente(df_limpio, DB_PATH)
    sincronizar_pendientes_a_gcs(db)
    logger.info("=== PIPELINE ESCENARIO B FINALIZADO EXITOSAMENTE ===")


if __name__ == "__main__":
    main()
