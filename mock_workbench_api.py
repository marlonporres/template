"""
SERVIDOR MOCK AUXILIAR EN FLASK (Simulador de API Origen y API Destino Workbench)
=================================================================================
Permite probar `template_3_rest_api_sync.py` de punta a punta de forma 100% local:
- GET  /api/source/records -> Simula la API REST origen paginada con casos válidos y sucios.
- POST /api/sync           -> Simula el endpoint destino que recibe registros limpios.
- GET  /api/status         -> Devuelve el estado del servidor y los registros sincronizados.

Uso:
  1. En la Terminal 1: python mock_workbench_api.py
  2. En la Terminal 2: python template_3_rest_api_sync.py
"""

from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

# Almacenamiento en memoria para inspección durante la prueba
REGISTROS_SINCRONIZADOS: list[dict] = []

DATOS_ORIGEN_MOCK = [
    {
        "id": "REC-5001",
        "account": {
            "id": " acc-101 ",
            "holder_name": "  Ana Gómez ",
            "contact": {"email": "ANA.GOMEZ@EMPRESA.COM "}
        },
        "financials": {"amount": "$1,850.50", "currency": "usd"},
        "status": "approved",
        "created_at": "2026-11-15T10:00:00Z"
    },
    {
        "id": "REC-5002",
        "account": {
            "id": "ACC-102",
            "holder_name": "Carlos Ruiz",
            "contact": {"email": "carlos.ruiz@empresa.com"}
        },
        "financials": {"amount": "3,400.00 USD", "currency": "USD"},
        "status": "APPROVED",
        "created_at": "2026-11-15T10:05:00Z"
    },
    {
        "id": "REC-5003",
        "account": {
            "id": "ACC-103",
            "holder_name": "Registro Sin Email",
            "contact": {"email": None}
        },
        "financials": {"amount": "950.00", "currency": "USD"},
        "status": "APPROVED",
        "created_at": "2026-11-15T10:10:00Z"
    },
    {
        "id": "REC-5004",
        "account": {
            "id": "ACC-104",
            "holder_name": "Registro Monto Inválido",
            "contact": {"email": "error@empresa.com"}
        },
        "financials": {"amount": "N/A", "currency": "USD"},
        "status": "APPROVED",
        "created_at": "2026-11-15T10:15:00Z"
    },
]


@app.route("/api/source/records", methods=["GET"])
def get_source_records():
    """Simula una API REST paginada de origen."""
    page = int(request.args.get("page", 1))
    if page > 1:
        return jsonify({"data": [], "page": page, "has_next": False}), 200
    return jsonify({
        "data": DATOS_ORIGEN_MOCK,
        "page": 1,
        "has_next": False
    }), 200


@app.route("/api/sync", methods=["POST"])
def sync_record():
    """Recibe registros validados desde `template_3_rest_api_sync.py`."""
    payload = request.get_json(silent=True) or {}
    record_id = payload.get("record_id")

    if not record_id:
        return jsonify({
            "status": "error",
            "message": "El campo 'record_id' es obligatorio."
        }), 400

    REGISTROS_SINCRONIZADOS.append({
        **payload,
        "received_by_workbench_at": datetime.now().isoformat(timespec="seconds")
    })
    return jsonify({
        "status": "synced",
        "record_id": record_id,
        "total_synced": len(REGISTROS_SINCRONIZADOS)
    }), 200


@app.route("/api/status", methods=["GET"])
def get_status():
    """Devuelve el estado actual del servidor mock y los registros recibidos."""
    return jsonify({
        "service": "Mock Workbench API",
        "status": "online",
        "total_records_received": len(REGISTROS_SINCRONIZADOS),
        "records": REGISTROS_SINCRONIZADOS
    }), 200


if __name__ == "__main__":
    print("Servidor Mock Workbench iniciado en http://127.0.0.1:5000")
    app.run(host="127.0.0.1", port=5000, debug=False)
