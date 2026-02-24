"""validators.py — Validation métier des lectures de capteurs."""
import logging
from datetime import datetime
from typing import List, Tuple

from src.models import SensorReading, ValidationError

logger = logging.getLogger("ingestion.validator")

# Plages de valeurs acceptables par type de capteur
VALUE_RANGES = {
    "temperature": (-50.0, 60.0),
    "humidity": (0.0, 100.0),
    "rainfall": (0.0, 500.0),
    "irrigation": (0.0, 200.0),
    "wind_speed": (0.0, 300.0),
}


def validate_single_reading(reading: SensorReading) -> List[ValidationError]:
    """Valide une lecture individuelle. Retourne la liste des erreurs."""
    errors = []
    sid = reading.sensor_id or "(vide)"

    # Vérifier sensor_id non vide
    if not reading.sensor_id or not reading.sensor_id.strip():
        errors.append(ValidationError(
            sensor_id=sid, field="sensor_id",
            message="Le sensor_id est obligatoire et ne peut être vide"))

    # Vérifier que value est numérique
    if not isinstance(reading.value, (int, float)):
        errors.append(ValidationError(
            sensor_id=sid, field="value",
            message=f"Valeur non numérique : {reading.value!r} "
                    f"(type={type(reading.value).__name__})"))
    else:
        # Vérifier la plage de valeurs selon le type
        range_tuple = VALUE_RANGES.get(reading.type)
        if range_tuple:
            vmin, vmax = range_tuple
            if not (vmin <= reading.value <= vmax):
                errors.append(ValidationError(
                    sensor_id=sid, field="value",
                    message=f"Valeur {reading.value} hors plage "
                            f"[{vmin}, {vmax}] pour type={reading.type}"))

    # Vérifier le timestamp
    try:
        datetime.fromisoformat(reading.timestamp)
    except (ValueError, TypeError):
        errors.append(ValidationError(
            sensor_id=sid, field="timestamp",
            message=f"Timestamp invalide : {reading.timestamp!r}"))

    # Vérifier cohérence irrigation / pompe
    if (reading.pump_status == "OFF"
            and reading.irrigation_mm is not None
            and reading.irrigation_mm > 0):
        errors.append(ValidationError(
            sensor_id=sid, field="pump_status",
            message=f"pump_status=OFF mais irrigation_mm="
                    f"{reading.irrigation_mm} > 0"))

    return errors


def validate_readings(
    readings: List[SensorReading],
) -> Tuple[List[SensorReading], List[ValidationError]]:
    """
    Valide une liste de lectures.
    Retourne (accepted, all_errors).
    """
    accepted = []
    all_errors = []

    for reading in readings:
        errs = validate_single_reading(reading)
        if errs:
            all_errors.extend(errs)
            logger.warning("Lecture rejetée [%s] : %d erreur(s)",
                           reading.sensor_id or "(vide)", len(errs))
        else:
            accepted.append(reading)
            logger.debug("Lecture acceptée [%s]", reading.sensor_id)

    logger.info("Validation terminée : %d acceptées, %d erreur(s)",
                len(accepted), len(all_errors))
    return accepted, all_errors