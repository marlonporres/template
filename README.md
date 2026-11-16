# Kit de Plantillas de Automatización y Migración de Datos (CRM / Operaciones)

Repositorio de plantillas modulares, idempotentes y listas para producción orientadas a flujos de extracción, limpieza, cruce relacional, persistencia de control y sincronización hacia la nube o APIs externas.

Todas las plantillas mantienen exactamente la **misma arquitectura de 5 capas**:
1. **Extracción Segura:** Lectura de archivos locales, Google Forms API o APIs REST paginadas usando `glom` y `requests`.
2. **Limpieza y Transformación:** Estandarización de textos y montos en `pandas` + cruces/reglas SQL en memoria con `duckdb`.
3. **Persistencia Idempotente:** Control de estados (`PENDING` -> `SYNCED`) y auditoría de rechazos en SQLite con `sqlite-utils`.
4. **Salida Resiliente:** Subida de artefactos a **Google Cloud Storage (GCS)** y/o envíos `POST` a APIs REST protegidos con reintentos exponenciales (`tenacity`).
5. **Observabilidad:** Bitácora estructurada en consola y archivo con `loguru`.

---

## 1. Configuración Rápida del Entorno Virtual (`.venv`) en Windows y VS Code

### Opción A: Desde la Paleta de Comandos de VS Code (Recomendado)
1. Abre la carpeta del proyecto en **Visual Studio Code**.
2. Presiona `Ctrl + Shift + P` para abrir la Paleta de Comandos.
3. Escribe y selecciona **`Python: Create Environment...`**.
4. Elige **`Venv`** -> selecciona tu intérprete de **Python 3.10+**.
5. Marca la casilla de **`requirements.txt`** y presiona **OK** (VS Code creará `.venv` e instalará todo automáticamente).

### Opción B: Desde la Terminal (`cmd` en Windows)
Abre una terminal en VS Code (`Ctrl + Ñ` o `Ctrl + ` `) asegurándote de usar **`Command Prompt (cmd)`**:

```cmd
:: 1. Crear el entorno virtual
python -m venv .venv

:: 2. Activar el entorno virtual en CMD
.venv\Scripts\activate.bat

:: 3. Actualizar pip e instalar las dependencias
python -m pip install --upgrade pip
pip install -r requirements.txt
```

> **Nota si usas PowerShell:** Actívalo con `.venv\Scripts\Activate.ps1`. Si aparece bloqueo de scripts, ejecuta primero:
> `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`

---

## 2. ¿Cuándo Usar Cada Plantilla?

| Archivo | Escenario de Entrada | Cuándo Usarla | Comando de Prueba |
| :--- | :--- | :--- | :--- |
| **`template_1_csv_json.py`** | **Escenario A:** Archivos locales `.csv` y `.json` anidados | Cuando recibes lotes planos o exportaciones de CRM/ERP que deben limpiarse, cruzarse entre sí (`INNER JOIN` en DuckDB), guardarse en SQLite y subirse a un bucket de GCS. | `python template_1_csv_json.py` |
| **`template_2_google_forms.py`** | **Escenario B:** Google Forms API (`forms.get` + `responses.list`) | Cuando los datos provienen de formularios operativos donde las preguntas pueden venir vacías y cada respuesta debe convertirse en un `.json` individual dentro de GCS sin duplicarse. | `python template_2_google_forms.py` |
| **`template_3_rest_api_sync.py`** | **Escenario C:** API REST paginada con Headers Bearer -> API Workbench | Cuando debes consumir un endpoint `GET`, separar registros corruptos en una tabla de auditoría (`registros_rechazados`) y enviar los válidos por `POST` hacia un sistema destino. | `python template_3_rest_api_sync.py` |
| **`mock_workbench_api.py`** | Servidor Auxiliar Flask (`localhost:5000`) | Cuando necesitas simular localmente tanto la API de origen (`/api/source/records`) como el endpoint destino (`POST /api/sync` y `GET /api/status`). | `python mock_workbench_api.py` |

---

## 3. Cheatsheet Rápido por Herramienta (5 Líneas Clave)

### 3.1 `glom` (Extracción segura de JSON anidado ante valores `null`)
```python
from glom import glom
acc_id = str(glom(item, "account_id", default="") or "").strip().upper()
email = str(glom(item, "client.email", default="") or "").strip().lower()
ans_val = str(glom(resp, f"answers.{q_id}.textAnswers.answers.0.value", default="") or "").strip()
items_api = glom(payload, "data.items", default=[]) or []
```

### 3.2 `pandas` (Lectura limpia y normalización de cadenas y dinero)
```python
import pandas as pd
df = pd.read_csv("datos_prueba/transacciones_sample.csv", encoding="utf-8-sig")
df["account_id"] = df["account_id"].astype(str).str.strip().str.upper()
df["amount"] = pd.to_numeric(df["amount"].astype(str).str.replace(r"[$,USD\s]", "", regex=True), errors="coerce")
df.to_csv("reporte_final.csv", index=False, encoding="utf-8")
```

### 3.3 `duckdb` (Cruces SQL en memoria directamente sobre DataFrames de Pandas)
```python
import duckdb
df_join = duckdb.sql("""
    SELECT t.tx_id, t.account_id, c.client_name, t.amount, ROUND(t.amount * 0.05, 2) AS fee
    FROM df_txs t INNER JOIN df_clientes c ON t.account_id = c.account_id
    WHERE t.amount IS NOT NULL AND t.amount >= 1000 AND c.status = 'ACTIVE'
""").df()
```

### 3.4 `sqlite-utils` (Persistencia idempotente y control de estado `PENDING` / `SYNCED`)
```python
from sqlite_utils import Database
db = Database("migracion_control.db")
db["transacciones"].insert_all(registros, pk="tx_id", alter=True, ignore=True)
pendientes = list(db["transacciones"].rows_where("sync_status = ?", ["PENDING"]))
db["transacciones"].update(tx_id, {"sync_status": "SYNCED"})
```

### 3.5 `google-cloud-storage` (Subida de archivos y JSON a Buckets de GCS)
```python
from google.cloud import storage
client = storage.Client.from_service_account_json("service_account.json")
bucket = client.bucket("nombre-del-bucket")
blob = bucket.blob(f"auditoria/{fecha}/{record_id}.json")
blob.upload_from_string(json.dumps(registro, indent=2), content_type="application/json")
```

### 3.6 `tenacity` (Reintentos automáticos con Backoff Exponencial ante fallos de red)
```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(requests.exceptions.RequestException), reraise=True)
def post_con_reintento(url: str, payload: dict) -> dict:
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()
```

### 3.7 `loguru` (Logging instantáneo en consola y archivo con rotación)
```python
from loguru import logger
logger.add("auditoria_ejecucion.log", rotation="10 MB", level="INFO", encoding="utf-8")
logger.info(f"Iniciando sincronización de {len(pendientes)} registros...")
logger.warning(f"Registro omitido por monto inválido: {record_id}")
logger.error(f"Fallo crítico al conectar con endpoint externo: {err}")
```

### 3.8 `google-cloud-storage` — Retrieval (Descargar archivos desde GCS a local)
```python
from google.cloud import storage
client = storage.Client.from_service_account_json("service_account.json")
blob = client.bucket("nombre-del-bucket").blob("carpeta_origen/clientes.csv")
blob.download_to_filename("clientes_descargado.csv")  # Descarga a disco local para leer con pd.read_csv()
# O descargar directo en memoria como texto/JSON y listar blobs de un prefijo:
contenido_json = json.loads(blob.download_as_text(encoding="utf-8"))
archivos = [b.name for b in client.list_blobs("nombre-del-bucket", prefix="carpeta_origen/")]
```

### 3.9 Consultas de Agregación y Lectura Nativa en SQLite (`sqlite3` + `pandas`)
```python
import sqlite3, pandas as pd
with sqlite3.connect("migracion_escenario_a.db") as conn:
    df_resumen = pd.read_sql_query("""
        SELECT sync_status, COUNT(*) AS total_registros, ROUND(SUM(amount), 2) AS monto_total
        FROM transacciones_conciliadas GROUP BY sync_status
    """, conn)
```

---

## 4. Buenas Prácticas Agile y Entrega de Evaluación Técnica

### 4.1 Estructura de Commits Sugerida (Conventional Commits)
Durante la prueba técnica, realiza commits pequeños e incrementales en lugar de un único commit al final:
1. `chore: initialize project structure, gitignore and requirements`
2. `feat(extract): implement safe nested json/api extraction with glom`
3. `feat(transform): add pandas normalization and duckdb sql validation`
4. `feat(db): add idempotent sqlite state tracking with sqlite-utils`
5. `feat(sync): integrate gcs upload and tenacity retry logic for workbench api`
6. `docs: document technical decisions and execution steps`

### 4.2 Plantilla para Documentar Decisiones Técnicas al Entregar
Copia y adapta este bloque en tu correo, PR o nota de entrega al finalizar la evaluación:

```markdown
## Resumen Ejecutivo de la Solución

1. **Extracción Defensiva (`glom` + `utf-8-sig`):**
   - Se utilizó `glom` para recorrer estructuras JSON anidadas y respuestas dinámicas sin riesgo de `KeyError` ante campos `null` o preguntas opcionales vacías.
   - Los archivos CSV se procesan con `encoding="utf-8-sig"` para neutralizar caracteres BOM generados por Excel.

2. **Calidad de Datos y Cruce Relacional (`pandas` + `duckdb`):**
   - Los montos monetarios se sanean mediante expresiones regulares y `pd.to_numeric(..., errors="coerce")`, aislando valores corruptos.
   - La lógica de negocio y el cruce entre entidades se expresan en SQL declarativo sobre memoria con `DuckDB`, facilitando mantenimiento y auditoría.

3. **Idempotencia y Trazabilidad (`sqlite-utils` + `loguru`):**
   - Cada registro se persiste en SQLite usando su llave primaria (`pk`) con `ignore=True` y estado inicial `PENDING`.
   - Si el proceso se interrumpe a mitad de lote y se vuelve a ejecutar, únicamente se procesan los registros `PENDING`, evitando duplicados en el sistema destino o en Google Cloud Storage.
   - Los registros que no cumplen las reglas de negocio se almacenan en `registros_rechazados` junto con su causa exacta (`rejection_reason`).

4. **Resiliencia en Red (`tenacity`):**
   - Las llamadas externas hacia APIs REST y Google Cloud Storage están decoradas con `@retry` y backoff exponencial para tolerar microcortes o errores transitorios (HTTP 5xx / Timeouts).
```

### 4.3 Technical Delivery Summary & Weekly Accountability Pitch (English Version)
Lista para pegar en el PR/correo en inglés o decir verbalmente durante la entrevista:

```markdown
## Technical Implementation Summary (English)

1. **Defensive Data Extraction (`glom` & `utf-8-sig`):**
   - Used `glom` to safely traverse nested JSON payloads and dynamic Google Forms responses, preventing `KeyError` or `TypeError` exceptions on `null` or missing optional fields.
   - Configured CSV ingestion with `encoding="utf-8-sig"` to strip potential Excel BOM characters automatically.

2. **Data Normalization & Relational Validation (`pandas` & `duckdb`):**
   - Sanitized currency fields via regex and `pd.to_numeric(..., errors="coerce")` to isolate malformed amounts without crashing the pipeline.
   - Executed declarative SQL joins and business rule validations in-memory using `DuckDB`, separating valid records from rejected ones (`registros_rechazados`) for full auditability.

3. **Transactional Idempotency (`sqlite-utils`):**
   - Persisted records into SQLite using primary keys (`ignore=True`) with an initial `PENDING` status.
   - Enforced atomic state transitions: records are updated to `SYNCED` strictly inside the `try` block after confirming successful upload to Google Cloud Storage or HTTP 200 response from the destination API.

4. **Fault Tolerance & Observability (`tenacity` & `loguru`):**
   - Wrapped external API calls with exponential backoff retries (`@retry`) and structured execution logs via `loguru` to ensure minimal downtime and rapid troubleshooting.
```

**Spoken Pitch (30 segundos para explicar tu código en inglés):**
> *"I structured the automation as an idempotent 4-step pipeline: First, I extract the raw data defensively using `glom` and `utf-8-sig` so null or missing fields never break execution. Second, I clean currency and string fields in `Pandas` and apply business rules using `DuckDB` SQL queries, isolating invalid rows into an audit table. Third, I track state in SQLite as `PENDING` using `sqlite-utils` with primary-key deduplication. Finally, I sync pending records to Google Cloud Storage or the REST API with `tenacity` retries, updating the status to `SYNCED` only after a confirmed upload."*

