"""protocol.py — Encodage, décodage et framing des messages NDJSON."""
import json
import socket
import uuid
from datetime import datetime

PROTOCOL_VERSION = "v1"
MAX_MESSAGE_SIZE = 1_048_576  # 1 Mo


def build_message(msg_type: str, payload: dict,
                  request_id: str | None = None) -> dict:
    """Construit un message conforme au protocole v1."""
    return {
        "version": PROTOCOL_VERSION,
        "type": msg_type,
        "request_id": request_id or str(uuid.uuid4()),
        "sent_at": datetime.now().isoformat(),
        "payload": payload,
    }


def encode_message(msg: dict) -> bytes:
    """Sérialise un dict en JSON compact + newline → bytes UTF-8."""
    line = json.dumps(msg, ensure_ascii=False, separators=(",", ":"))
    return (line + "\n").encode("utf-8")


def decode_message(raw: str) -> dict:
    """Parse une ligne JSON en dict Python."""
    stripped = raw.strip()
    if not stripped:
        raise ValueError("Message vide")
    return json.loads(stripped)


def recv_line(conn: socket.socket, buffer: bytearray,
              max_size: int = MAX_MESSAGE_SIZE) -> str | None:
    """
    Lit le socket et accumule dans buffer.
    Retourne la première ligne complète (str) ou None si connexion fermée.
    """
    while True:
        # Chercher un '\n' dans le buffer existant
        newline_pos = buffer.find(b"\n")
        if newline_pos != -1:
            line = buffer[:newline_pos].decode("utf-8")
            del buffer[:newline_pos + 1]
            return line

        # Pas de ligne complète → lire plus de données
        try:
            chunk = conn.recv(4096)
        except socket.timeout:
            raise
        if not chunk:
            # Connexion fermée
            if buffer:
                # Retourner ce qui reste comme dernière ligne
                line = buffer[:].decode("utf-8")
                buffer.clear()
                return line
            return None

        buffer.extend(chunk)

        # Protection contre les messages trop volumineux
        if len(buffer) > max_size:
            raise ValueError(
                f"Message trop volumineux (> {max_size} octets)")