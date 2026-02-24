"""main_demo.py — Point d'entrée démo et vérifications rapides."""
import json
from src.models import SensorReading, IngestRequest, ValidationError
from src.validators import validate_single_reading
from src.protocol import encode_message, decode_message, build_message

def run_checks():
    """Exécute des vérifications simples."""
    print("=== Vérifications rapides ===\n")

    # Check 1 : encode / decode
    msg = build_message("ping", {"status": "alive"})
    encoded = encode_message(msg)
    decoded = decode_message(encoded.decode("utf-8"))
    assert decoded["type"] == "ping", "Échec encode/decode"
    assert decoded["payload"]["status"] == "alive"
    print("✅ Check 1 : encode/decode OK")

    # Check 2 : lecture valide
    valid_reading = SensorReading(
        sensor_id="t01", type="temperature", value=22.0,
        unit="°C", timestamp="2026-02-23T10:00:00")
    errs = validate_single_reading(valid_reading)
    assert len(errs) == 0, f"Attendu 0 erreur, obtenu {len(errs)}"
    print("✅ Check 2 : lecture valide OK")

    # Check 3 : lecture invalide (valeur aberrante)
    bad_reading = SensorReading(
        sensor_id="t02", type="temperature", value=-999.0,
        unit="°C", timestamp="2026-02-23T10:00:00")
    errs = validate_single_reading(bad_reading)
    assert len(errs) > 0, "Attendu au moins 1 erreur"
    print("✅ Check 3 : lecture invalide détectée OK")

    # Check 4 : sensor_id vide
    empty_id = SensorReading(
        sensor_id="", type="humidity", value=50.0,
        unit="%", timestamp="2026-02-23T10:00:00")
    errs = validate_single_reading(empty_id)
    assert any(e.field == "sensor_id" for e in errs)
    print("✅ Check 4 : sensor_id vide détecté OK")

    # Check 5 : cohérence pompe/irrigation
    pump_off = SensorReading(
        sensor_id="i01", type="irrigation", value=5.0,
        unit="mm", timestamp="2026-02-23T10:00:00",
        pump_status="OFF", irrigation_mm=5.0)
    errs = validate_single_reading(pump_off)
    assert any(e.field == "pump_status" for e in errs)
    print("✅ Check 5 : incohérence pompe/irrigation détectée OK")

    # Check 6 : message protocolaire complet
    ingest_req = IngestRequest(source="test", readings=[valid_reading])
    proto_msg = build_message("ingest_request", ingest_req.to_dict())
    assert proto_msg["version"] == "v1"
    assert proto_msg["type"] == "ingest_request"
    assert "request_id" in proto_msg
    assert "sent_at" in proto_msg
    print("✅ Check 6 : structure protocolaire OK")

    print("\n=== Toutes les vérifications passées ! ===")


if __name__ == "__main__":
    run_checks()