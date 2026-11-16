import json
import pandas as pd
import duckdb
from glom import glom
from loguru import logger
from sqlite_utils import Database
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage

# === ZONA DE CAMBIOS EN CALIENTE (CONFIGURACIÓN) ===
SERVICE_ACCOUNT_FILE = "service_account.json"
FORM_ID = "1fKnbT-kz4w3thIZCwZXHIvKCnZRk6AVaZuKghkoCSl4"
DB_NAME = "migracion_escenario_b.db"
TABLE_NAME = "respuestas_formulario"
PK_COLUMN = "response_id"
BUCKET_NAME = "bucket-de-prueba-icon"
GCS_FOLDER = "forms_responses"
SCOPES = ["https://www.googleapis.com/auth/forms.body.readonly", "https://www.googleapis.com/auth/forms.responses.readonly"]

logger.add("auditoria_escenario_b.log", level="INFO", encoding="utf-8")

def ejecutar_pipeline():
    logger.info("1. Conectando a Google Forms API...")
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("forms", "v1", credentials=creds, cache_discovery=False)

    # >>> CAMBIO EN CALIENTE AQUÍ: Mapeo dinámico de preguntas {questionId: nombre_columna} con glom
    meta = service.forms().get(formId=FORM_ID).execute()
    mapa_preguntas = {}
    for item in glom(meta, "items", default=[]):
        q_id = glom(item, "questionItem.question.questionId", default=None)
        titulo = str(glom(item, "title", default="") or "").strip().lower().replace(" ", "_")
        if q_id:
            mapa_preguntas[q_id] = titulo

    res = service.forms().responses().list(formId=FORM_ID).execute()
    filas = []
    for r in glom(res, "responses", default=[]):
        reg = {"response_id": glom(r, "responseId", default=""), "submitted_at": glom(r, "lastSubmittedTime", default="")}
        for q_id, col in mapa_preguntas.items():
            reg[col] = str(glom(r, f"answers.{q_id}.textAnswers.answers.0.value", default="") or "").strip()
        filas.append(reg)

    # >>> CAMBIO EN CALIENTE AQUÍ: Limpieza en Pandas y filtro/cálculo SQL en DuckDB
    df = pd.DataFrame(filas)
    if df.empty:
        return logger.warning("No se encontraron respuestas en el formulario.")
    df_final = duckdb.sql("SELECT *, 'PENDING' AS sync_status FROM df WHERE response_id != ''").df()

    # >>> CAMBIO EN CALIENTE AQUÍ: Llave primaria (pk) e inserción idempotente en SQLite
    db = Database(DB_NAME)
    tabla = db[TABLE_NAME]
    tabla.insert_all(df_final.to_dict(orient="records"), pk=PK_COLUMN, alter=True, ignore=True)

    pendientes = list(tabla.rows_where("sync_status = ?", ["PENDING"]))
    if pendientes:
        client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        bucket = client.bucket(BUCKET_NAME)
        for item in pendientes:
            resp_id = item[PK_COLUMN]
            try:
                blob = bucket.blob(f"{GCS_FOLDER}/{resp_id}.json")
                blob.upload_from_string(json.dumps(item, indent=2, ensure_ascii=False), content_type="application/json")
                tabla.update(resp_id, {"sync_status": "SYNCED"}, alter=True)
            except Exception as e:
                logger.error(f"Error subiendo {resp_id} a GCS (permanece en PENDING): {e}")
    logger.info(f"Proceso completado. Pendientes procesados: {len(pendientes)}")

if __name__ == "__main__":
    ejecutar_pipeline()
