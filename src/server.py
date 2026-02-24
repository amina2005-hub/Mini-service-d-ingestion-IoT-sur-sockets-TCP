"""server.py — Serveur TCP d'ingestion IoT."""
import socket
import logging
import argparse
import time

from src.models import IngestRequest, IngestResponse, SensorReading
from src.validators import validate_readings
from src.protocol import (
    recv_line, decode_message, encode_message, build_message
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingestion.server")


def handle_client(conn: socket.socket, addr: tuple):
    """Traite une connexion client unique."""
    buffer = bytearray()
    request_id = "unknown"
    start_time = time.time()

    try:
        conn.settimeout(30.0)

        # Lire un message NDJSON
        line = recv_line(conn, buffer)
        if line is None:
            logger.warning("Connexion fermée immédiatement par %s", addr)
            return

        # Décoder le message protocolaire
        msg = decode_message(line)
        request_id = msg.get("request_id", "unknown")
        msg_type = msg.get("type", "")
        logger.info("[%s] Message reçu : type=%s depuis %s",
                    request_id, msg_type, addr)

        if msg_type != "ingest_request":
            error_resp = build_message("error",
                {"message": f"Type non supporté : {msg_type}"},
                request_id=request_id)
            conn.sendall(encode_message(error_resp))
            return

        # Extraire et reconstruire la requête métier
        payload = msg.get("payload", {})
        ingest_req = IngestRequest.from_dict(payload)
        logger.info("[%s] %d lectures à valider (source=%s)",
                    request_id, len(ingest_req.readings), ingest_req.source)

        # Valider les lectures
        accepted, errors = validate_readings(ingest_req.readings)
        elapsed_ms = (time.time() - start_time) * 1000

        # Construire la réponse
        response = IngestResponse(
            request_id=request_id,
            accepted_count=len(accepted),
            rejected_count=len(errors),
            errors=errors,
            processing_time_ms=round(elapsed_ms, 2),
        )

        resp_msg = build_message("ingest_response",
                                 response.to_dict(),
                                 request_id=request_id)
        conn.sendall(encode_message(resp_msg))

        logger.info("[%s] Réponse envoyée : accepted=%d, rejected=%d, "
                    "time=%.2fms",
                    request_id, response.accepted_count,
                    response.rejected_count, elapsed_ms)

    except socket.timeout:
        logger.error("[%s] Timeout client %s", request_id, addr)
    except (ValueError, KeyError) as e:
        logger.error("[%s] Erreur de parsing : %s", request_id, e)
        try:
            err_msg = build_message("error",
                {"message": str(e)}, request_id=request_id)
            conn.sendall(encode_message(err_msg))
        except OSError:
            pass
    except OSError as e:
        logger.error("[%s] Erreur réseau : %s", request_id, e)
    finally:
        conn.close()


def run_server(host: str = "127.0.0.1", port: int = 9000):
    """Lance le serveur TCP."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(5)
        logger.info("🚀 Serveur en écoute sur %s:%d", host, port)

        while True:
            try:
                conn, addr = srv.accept()
                logger.info("Connexion acceptée depuis %s:%d",
                            addr[0], addr[1])
                handle_client(conn, addr)
            except KeyboardInterrupt:
                logger.info("Arrêt du serveur (Ctrl+C)")
                break
            except OSError as e:
                logger.error("Erreur accept : %s", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serveur d'ingestion IoT")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    run_server(args.host, args.port)