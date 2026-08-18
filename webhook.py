from flask import Flask, request, send_file, jsonify
import csv
import json
import os
import hmac
import sqlite3
import requests
import time
from datetime import datetime, timedelta
from threading import Thread

app = Flask(__name__)
    
fichier = "journal_trades.csv"

GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbyVUSrnvHzkFTyuMqZfjEvXbOe2_bkFRsCYbdtfXuR3MZgGJePgh5vF9-eqeHnCeDCq/exec"

GOOGLE_SCRIPT_SECRET = os.getenv("GOOGLE_SCRIPT_SECRET", "").strip()

# ============================================================
# QROS STEP 45E.2A
# DURABLE DELIVERY QUEUE — SQLITE PERSISTENT REPOSITORY
# ============================================================

QROS_QUEUE_DB_PATH = os.getenv(
    "QROS_QUEUE_DB_PATH",
    "/data/qros_delivery_queue.db"
).strip()


# ============================================================
# QROS STEP 45E.9-A
# WEBHOOK HARDENING — REQUEST BODY SIZE LIMIT
# ============================================================

QROS_WEBHOOK_MAX_BODY_BYTES = int(
    os.getenv(
        "QROS_WEBHOOK_MAX_BODY_BYTES",
        "65536"
    )
)


# ============================================================
# QROS STEP 45E.9-D
# WEBHOOK HARDENING — INGRESS AUTHENTICATION
#
# DEFAULT MODE: SHADOW
# - OFF: no ingress secret validation.
# - SHADOW: validates/logs but does not reject.
# - ENFORCE: rejects missing/invalid ingress secret.
# ============================================================

QROS_WEBHOOK_AUTH_MODE = os.getenv(
    "QROS_WEBHOOK_AUTH_MODE",
    "SHADOW"
).strip().upper()

QROS_WEBHOOK_INGRESS_SECRET = os.getenv(
    "QROS_WEBHOOK_INGRESS_SECRET",
    ""
).strip()


# ============================================================
# QROS STEP 45E.10-A
# CONTROLLED CUTOVER — DELIVERY PATH MODE
#
# DEFAULT: LEGACY
# - LEGACY: preserves current production delivery path.
# - DURABLE_QUEUE: routes accepted events to durable queue.
# - No automatic cutover.
# ============================================================

QROS_DELIVERY_PATH_MODE = os.getenv(
    "QROS_DELIVERY_PATH_MODE",
    "LEGACY"
).strip().upper()


# ============================================================
# QROS STEP 45E.3C-B
# AUTOMATIC QUEUE WORKER — CONTROL SETTINGS
#
# DEFAULT OFF
# - Activation will be explicit through Railway Variables.
# - No behavior change while disabled.
# ============================================================

QROS_QUEUE_WORKER_ENABLED = os.getenv(
    "QROS_QUEUE_WORKER_ENABLED",
    "false"
).strip().lower() in (
    "1",
    "true",
    "yes",
    "on"
)

QROS_QUEUE_WORKER_INTERVAL_SECONDS = float(
    os.getenv(
        "QROS_QUEUE_WORKER_INTERVAL_SECONDS",
        "5"
    )
)

QROS_QUEUE_WORKER_MAX_EVENTS_PER_CYCLE = int(
    os.getenv(
        "QROS_QUEUE_WORKER_MAX_EVENTS_PER_CYCLE",
        "10"
    )
)


# ============================================================
# QROS STEP 45E.11-G
# PRODUCTION GUARDRAILS — CONFIGURABLE HEALTH THRESHOLDS
#
# Defaults preserve the currently certified behavior.
# Railway Variables can override them without code changes.
# ============================================================

QROS_HEALTH_PENDING_DEGRADED = int(
    os.getenv("QROS_HEALTH_PENDING_DEGRADED", "5")
)

QROS_HEALTH_PENDING_CRITICAL = int(
    os.getenv("QROS_HEALTH_PENDING_CRITICAL", "20")
)

QROS_HEALTH_RETRY_CRITICAL = int(
    os.getenv("QROS_HEALTH_RETRY_CRITICAL", "10")
)

QROS_HEALTH_DEAD_LETTER_DEGRADED = int(
    os.getenv("QROS_HEALTH_DEAD_LETTER_DEGRADED", "10")
)

QROS_HEALTH_DEAD_LETTER_HIGH = int(
    os.getenv("QROS_HEALTH_DEAD_LETTER_HIGH", "20")
)

QROS_HEALTH_PENDING_STALE_SECONDS = int(
    os.getenv("QROS_HEALTH_PENDING_STALE_SECONDS", "60")
)

QROS_HEALTH_PENDING_STUCK_SECONDS = int(
    os.getenv("QROS_HEALTH_PENDING_STUCK_SECONDS", "300")
)

QROS_HEALTH_WORKER_HEARTBEAT_STALE_SECONDS = int(
    os.getenv("QROS_HEALTH_WORKER_HEARTBEAT_STALE_SECONDS", "30")
)

# ============================================================
# QROS STEP 45E.4-G
# DURABLE RETRY — BACKOFF SETTINGS
#
# Persistent retry delays between worker attempts.
# Does not modify STEP45D in-memory retry yet.
# ============================================================

QROS_QUEUE_RETRY_DELAYS_SECONDS = (
    30,
    120,
    300
)

def qros_queue_get_retry_delay_seconds(attempt_count):
    attempt_count = int(attempt_count)

    if attempt_count <= 0:
        return QROS_QUEUE_RETRY_DELAYS_SECONDS[0]

    retry_index = attempt_count - 1

    if retry_index >= len(QROS_QUEUE_RETRY_DELAYS_SECONDS):
        return None

    return QROS_QUEUE_RETRY_DELAYS_SECONDS[retry_index]

def qros_queue_calculate_next_retry_at(attempt_count):
    retry_delay_seconds = qros_queue_get_retry_delay_seconds(
        attempt_count
    )

    if retry_delay_seconds is None:
        return None

    retry_at = (
        datetime.utcnow()
        + timedelta(seconds=retry_delay_seconds)
    )

    return (
        retry_at.isoformat(timespec="milliseconds")
        + "Z"
    )

# ============================================================
# QROS STEP 45B.1
# Railway Payload Contract Guard
# Version: 0.1.0
# Default mode: SHADOW
# ============================================================

QROS_PAYLOAD_GUARD_MODE = os.getenv(
    "QROS_PAYLOAD_GUARD_MODE",
    "SHADOW"
).strip().upper()

QROS_SUPPORTED_VERSION = "QROS_V49.0.1"

QROS_ALLOWED_RESULTS = {
    "OPEN",
    "WIN",
    "LOSS",
    "BE"
}

QROS_ALLOWED_DIRECTIONS = {
    "LONG",
    "SHORT"
}

QROS_ALLOWED_ACTIONS = {
    "buy",
    "sell"
}


def _qros_non_empty_string(value):
    return (
        isinstance(value, str)
        and bool(value.strip())
    )


def _qros_number_like(value):
    """
    Accept real numeric values and numeric strings.

    This is intentional because the current Pine V49 contract
    sends some price fields as JSON strings.
    """

    if isinstance(value, bool):
        return False

    try:
        number = float(value)
    except (TypeError, ValueError):
        return False

    return (
        number == number
        and number not in (
            float("inf"),
            float("-inf")
        )
    )


def validate_qros_v49_payload(data):
    """
    STEP 45B contract validation.

    Validation only.
    No mutation.
    No persistence.
    No Google call.

    Returns:
        {
            "valid": bool,
            "errors": [...],
            "warnings": [...]
        }
    """

    errors = []
    warnings = []

    # --------------------------------------------------------
    # 1. ROOT CONTRACT
    # --------------------------------------------------------

    if not isinstance(data, dict):
        return {
            "valid": False,
            "errors": [
                "PAYLOAD_NOT_OBJECT"
            ],
            "warnings": []
        }

    # --------------------------------------------------------
    # 2. REQUIRED IDENTITY
    # --------------------------------------------------------

    trade_id = data.get("trade_id")

    if not _qros_non_empty_string(trade_id):
        errors.append(
            "TRADE_ID_MISSING_OR_EMPTY"
        )

    elif not trade_id.startswith("QTR1_"):
        warnings.append(
            "TRADE_ID_NON_STANDARD_PREFIX"
        )

    version = data.get("version")

    if version != QROS_SUPPORTED_VERSION:
        errors.append(
            "UNSUPPORTED_VERSION"
        )

    # --------------------------------------------------------
    # 3. ASSET CONTRACT
    # --------------------------------------------------------

    symbol = data.get("symbol")
    asset = data.get("asset")

    if not _qros_non_empty_string(symbol):
        errors.append(
            "SYMBOL_MISSING_OR_EMPTY"
        )

    if asset is not None:

        if not _qros_non_empty_string(asset):
            errors.append(
                "ASSET_INVALID"
            )

        elif (
            _qros_non_empty_string(symbol)
            and asset != symbol
        ):
            errors.append(
                "SYMBOL_ASSET_MISMATCH"
            )

    # --------------------------------------------------------
    # 4. TIMEFRAME CONTRACT
    # --------------------------------------------------------

    timeframe = data.get("timeframe")
    tf = data.get("tf")

    if not _qros_non_empty_string(timeframe):
        errors.append(
            "TIMEFRAME_MISSING_OR_EMPTY"
        )

    if tf is not None:

        if not _qros_non_empty_string(tf):
            errors.append(
                "TF_INVALID"
            )

        elif (
            _qros_non_empty_string(timeframe)
            and tf != timeframe
        ):
            errors.append(
                "TIMEFRAME_TF_MISMATCH"
            )

    # --------------------------------------------------------
    # 5. DIRECTION / ACTION CONTRACT
    # --------------------------------------------------------

    direction = data.get("direction")

    if direction not in QROS_ALLOWED_DIRECTIONS:
        errors.append(
            "INVALID_DIRECTION"
        )

    action = data.get("action")

    if action not in QROS_ALLOWED_ACTIONS:
        errors.append(
            "INVALID_ACTION"
        )

    if (
        direction == "LONG"
        and action != "buy"
    ):
        errors.append(
            "LONG_ACTION_MISMATCH"
        )

    if (
        direction == "SHORT"
        and action != "sell"
    ):
        errors.append(
            "SHORT_ACTION_MISMATCH"
        )

    # --------------------------------------------------------
    # 6. RESULT CONTRACT
    # --------------------------------------------------------

    result = data.get("result")

    if result not in QROS_ALLOWED_RESULTS:
        errors.append(
            "INVALID_RESULT"
        )

    # --------------------------------------------------------
    # 7. REQUIRED TRADE NUMERICS
    # --------------------------------------------------------

    required_numeric_fields = (
        "entry",
        "sl",
        "tp",
        "rr",
        "score"
    )

    for field_name in required_numeric_fields:

        value = data.get(field_name)

        if not _qros_number_like(value):
            errors.append(
                "INVALID_NUMERIC_" +
                field_name.upper()
            )

    # --------------------------------------------------------
    # 8. BASIC PRICE SANITY
    # --------------------------------------------------------

    if _qros_number_like(data.get("entry")):

        if float(data["entry"]) <= 0:
            errors.append(
                "ENTRY_NOT_POSITIVE"
            )

    if _qros_number_like(data.get("sl")):

        if float(data["sl"]) <= 0:
            errors.append(
                "SL_NOT_POSITIVE"
            )

    if _qros_number_like(data.get("tp")):

        if float(data["tp"]) <= 0:
            errors.append(
                "TP_NOT_POSITIVE"
            )

    if _qros_number_like(data.get("rr")):

        if float(data["rr"]) <= 0:
            errors.append(
                "RR_NOT_POSITIVE"
            )

    # --------------------------------------------------------
    # 9. ENTRY GEOMETRY
    #
    # LONG:
    # SL < ENTRY < TP
    #
    # SHORT:
    # TP < ENTRY < SL
    # --------------------------------------------------------

    if all(
        _qros_number_like(data.get(name))
        for name in (
            "entry",
            "sl",
            "tp"
        )
    ):

        entry = float(
            data["entry"]
        )

        sl = float(
            data["sl"]
        )

        tp = float(
            data["tp"]
        )

        if direction == "LONG":

            if not (
                sl < entry < tp
            ):
                errors.append(
                    "INVALID_LONG_PRICE_GEOMETRY"
                )

        elif direction == "SHORT":

            if not (
                tp < entry < sl
            ):
                errors.append(
                    "INVALID_SHORT_PRICE_GEOMETRY"
                )

    # --------------------------------------------------------
    # 10. LEARNING IDENTITY SUPPORT
    # --------------------------------------------------------

    fingerprint = data.get(
        "fingerprint"
    )

    if not _qros_non_empty_string(
        fingerprint
    ):
        warnings.append(
            "FINGERPRINT_MISSING_OR_EMPTY"
        )

    setup = data.get(
        "setup"
    )

    if not _qros_non_empty_string(
        setup
    ):
        warnings.append(
            "SETUP_MISSING_OR_EMPTY"
        )

    # --------------------------------------------------------
    # FINAL
    # --------------------------------------------------------

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }

# ============================================================
# QROS STEP 45E.2A
# DURABLE DELIVERY QUEUE — SQLITE INITIALIZATION
#
# SHADOW / INFRASTRUCTURE ONLY
# - Does not modify /webhook behavior
# - Does not enqueue events yet
# - Does not call Google
# ============================================================

def qros_init_delivery_queue_db():
    os.makedirs(
        os.path.dirname(QROS_QUEUE_DB_PATH),
        exist_ok=True
    )

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        connection.execute(
            "PRAGMA synchronous = FULL"
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS qros_delivery_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                delivery_event_key TEXT NOT NULL UNIQUE,
                trade_id TEXT NOT NULL,
                event_phase TEXT NOT NULL,

                payload_json TEXT NOT NULL,

                status TEXT NOT NULL DEFAULT 'PENDING',

                attempt_count INTEGER NOT NULL DEFAULT 0,

                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                delivered_at TEXT,
                last_error TEXT
            )
            """
        )

        existing_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(qros_delivery_queue)"
            ).fetchall()
        }

        if "next_retry_at" not in existing_columns:
            connection.execute(
                """
                ALTER TABLE qros_delivery_queue
                ADD COLUMN next_retry_at TEXT
                """
            )

        if "claimed_at" not in existing_columns:
            connection.execute(
                """
                ALTER TABLE qros_delivery_queue
                ADD COLUMN claimed_at TEXT
                """
            )

        if "lease_until" not in existing_columns:
            connection.execute(
                """
                ALTER TABLE qros_delivery_queue
                ADD COLUMN lease_until TEXT
                """
            )

        if "worker_id" not in existing_columns:
            connection.execute(
                """
                ALTER TABLE qros_delivery_queue
                ADD COLUMN worker_id TEXT
                """
            )
        
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_qros_delivery_queue_status
            ON qros_delivery_queue(status)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_qros_delivery_queue_trade_id
            ON qros_delivery_queue(trade_id)
            """
        )

        connection.commit()

    finally:
        connection.close()


qros_init_delivery_queue_db()

# ============================================================
# QROS STEP 45E.2B
# DURABLE DELIVERY QUEUE — REPOSITORY FUNCTIONS
#
# REPOSITORY ONLY
# - Does not modify /webhook behavior
# - Does not call Google
# - Does not start workers
# ============================================================

def qros_queue_now_iso():
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


def qros_queue_build_delivery_event_key(data):
    trade_id = str(
        data.get("trade_id", "")
    ).strip()

    event_phase = str(
        data.get("result")
        or data.get("resultat")
        or ""
    ).strip().upper()

    if not trade_id or not event_phase:
        return ""

    return f"{trade_id}|{event_phase}"


def qros_queue_get_event(delivery_event_key):
    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        row = connection.execute(
            """
            SELECT
                id,
                delivery_event_key,
                trade_id,
                event_phase,
                payload_json,
                status,
                attempt_count,
                created_at,
                updated_at,
                delivered_at,
                last_error,
                next_retry_at,
                claimed_at,
                lease_until,
                worker_id        
            FROM qros_delivery_queue
            WHERE delivery_event_key = ?
            """,
            (
                delivery_event_key,
            )
        ).fetchone()

        if row is None:
            return None

        return dict(row)

    finally:
        connection.close()


def qros_queue_enqueue(data):
    delivery_event_key = (
        qros_queue_build_delivery_event_key(data)
    )

    trade_id = str(
        data.get("trade_id", "")
    ).strip()

    event_phase = str(
        data.get("result")
        or data.get("resultat")
        or ""
    ).strip().upper()

    if (
        not delivery_event_key
        or not trade_id
        or not event_phase
    ):
        return {
            "enqueued": False,
            "duplicate": False,
            "status": "INVALID_DELIVERY_IDENTITY",
            "delivery_event_key": delivery_event_key
        }

    payload_json = json.dumps(
        data,
        separators=(",", ":"),
        sort_keys=True
    )

    now = qros_queue_now_iso()

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        try:
            cursor = connection.execute(
                """
                INSERT INTO qros_delivery_queue (
                    delivery_event_key,
                    trade_id,
                    event_phase,
                    payload_json,
                    status,
                    attempt_count,
                    created_at,
                    updated_at,
                    delivered_at,
                    last_error
                )
                VALUES (?, ?, ?, ?, 'PENDING', 0, ?, ?, NULL, NULL)
                """,
                (
                    delivery_event_key,
                    trade_id,
                    event_phase,
                    payload_json,
                    now,
                    now
                )
            )

            connection.commit()

            return {
                "enqueued": True,
                "duplicate": False,
                "status": "PENDING",
                "delivery_event_key": delivery_event_key,
                "row_id": cursor.lastrowid
            }

        except sqlite3.IntegrityError:
            existing = qros_queue_get_event(
                delivery_event_key
            )

            return {
                "enqueued": False,
                "duplicate": True,
                "status": (
                    existing["status"]
                    if existing
                    else "DUPLICATE"
                ),
                "delivery_event_key": delivery_event_key,
                "row_id": (
                    existing["id"]
                    if existing
                    else None
                )
            }

    finally:
        connection.close()


def qros_queue_list_pending(limit=100):
    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        rows = connection.execute(
            """
            SELECT
                id,
                delivery_event_key,
                trade_id,
                event_phase,
                payload_json,
                status,
                attempt_count,
                created_at,
                updated_at,
                delivered_at,
                last_error,
                next_retry_at
            FROM qros_delivery_queue
            WHERE status = 'PENDING'
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                int(limit),
            )
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.4-C
# DURABLE RETRY — READY EVENT SELECTION
#
# READ ONLY
# - Fresh PENDING events are immediately eligible.
# - Scheduled retries are eligible only when next_retry_at
#   has been reached.
# - Existing worker is NOT switched to this function yet.
# ============================================================

def qros_queue_list_ready(limit=100):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        rows = connection.execute(
            """
            SELECT
                id,
                delivery_event_key,
                trade_id,
                event_phase,
                payload_json,
                status,
                attempt_count,
                created_at,
                updated_at,
                delivered_at,
                last_error,
                next_retry_at
            FROM qros_delivery_queue
            WHERE status = 'PENDING'
              AND (
                    next_retry_at IS NULL
                    OR next_retry_at <= ?
              )
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                now,
                int(limit),
            )
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.7-C
# CONCURRENCY SAFETY — ATOMIC READY EVENT CLAIM
#
# REPOSITORY ONLY
# - Claims one READY PENDING event atomically.
# - Moves it to PROCESSING.
# - Stores worker identity and lease metadata.
# - Does not call Google.
# - Existing worker is NOT switched to this function yet.
# ============================================================

def qros_queue_claim_one_ready(
    worker_id,
    lease_seconds=120
):

    worker_id = str(worker_id).strip()

    if not worker_id:
        return {
            "claimed": False,
            "status": "INVALID_WORKER_ID"
        }

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        connection.execute(
            "BEGIN IMMEDIATE"
        )

        now = qros_queue_now_iso()

        lease_until = (
            datetime.utcnow()
            + timedelta(
                seconds=int(lease_seconds)
            )
        ).isoformat(
            timespec="milliseconds"
        ) + "Z"

        row = connection.execute(
            """
            SELECT
                id,
                delivery_event_key
            FROM qros_delivery_queue
            WHERE status = 'PENDING'
              AND (
                    next_retry_at IS NULL
                    OR next_retry_at <= ?
              )
            ORDER BY id ASC
            LIMIT 1
            """,
            (
                now,
            )
        ).fetchone()

        if row is None:
            connection.commit()

            return {
                "claimed": False,
                "status": "NO_READY_EVENT"
            }

        cursor = connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                status = 'PROCESSING',
                updated_at = ?,
                claimed_at = ?,
                lease_until = ?,
                worker_id = ?
            WHERE id = ?
              AND status = 'PENDING'
            """,
            (
                now,
                now,
                lease_until,
                worker_id,
                row["id"]
            )
        )

        if cursor.rowcount != 1:
            connection.rollback()

            return {
                "claimed": False,
                "status": "CLAIM_LOST"
            }

        claimed_row = connection.execute(
            """
            SELECT
                id,
                delivery_event_key,
                trade_id,
                event_phase,
                payload_json,
                status,
                attempt_count,
                created_at,
                updated_at,
                delivered_at,
                last_error,
                next_retry_at,
                claimed_at,
                lease_until,
                worker_id
            FROM qros_delivery_queue
            WHERE id = ?
            """,
            (
                row["id"],
            )
        ).fetchone()

        connection.commit()

        return {
            "claimed": True,
            "status": "PROCESSING",
            "event": dict(claimed_row)
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.7-D
# CONCURRENCY SAFETY — EXPIRED LEASE RECOVERY
#
# REPOSITORY ONLY
# - Recovers PROCESSING events whose lease has expired.
# - Moves them back to PENDING.
# - Clears claim metadata.
# - Preserves attempt_count, payload and retry metadata.
# - Does not call Google.
# ============================================================

def qros_queue_recover_expired_leases(limit=100):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        connection.execute(
            "BEGIN IMMEDIATE"
        )

        now = qros_queue_now_iso()

        rows = connection.execute(
            """
            SELECT id
            FROM qros_delivery_queue
            WHERE status = 'PROCESSING'
              AND lease_until IS NOT NULL
              AND lease_until <= ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                now,
                int(limit),
            )
        ).fetchall()

        recovered_count = 0

        for row in rows:
            cursor = connection.execute(
                """
                UPDATE qros_delivery_queue
                SET
                    status = 'PENDING',
                    updated_at = ?,
                    claimed_at = NULL,
                    lease_until = NULL,
                    worker_id = NULL
                WHERE id = ?
                  AND status = 'PROCESSING'
                  AND lease_until IS NOT NULL
                  AND lease_until <= ?
                """,
                (
                    now,
                    row[0],
                    now
                )
            )

            recovered_count += cursor.rowcount

        connection.commit()

        return {
            "recovered_count": recovered_count,
            "status": (
                "RECOVERED"
                if recovered_count > 0
                else "NO_EXPIRED_LEASE"
            )
        }

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
        

def qros_queue_mark_attempt(
    delivery_event_key,
    error_message=None
):
    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                attempt_count = attempt_count + 1,
                updated_at = ?,
                last_error = ?
            WHERE delivery_event_key = ?
            """,
            (
                now,
                error_message,
                delivery_event_key
            )
        )

        connection.commit()

    finally:
        connection.close()


def qros_queue_mark_delivered(
    delivery_event_key
):
    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                status = 'DELIVERED',
                updated_at = ?,
                delivered_at = ?,
                last_error = NULL,
                next_retry_at = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                worker_id = NULL
            WHERE delivery_event_key = ?
            """,
            (
                now,
                now,
                delivery_event_key
            )
        )

        connection.commit()

    finally:
        connection.close()


def qros_queue_mark_failed(
    delivery_event_key,
    error_message
):
    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                status = 'FAILED',
                updated_at = ?,
                last_error = ?,
                next_retry_at = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                worker_id = NULL           
            WHERE delivery_event_key = ?
            """,
            (
                now,
                str(error_message),
                delivery_event_key
            )
        )

        connection.commit()

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.5-A
# DEAD LETTER QUEUE — MARK DEAD LETTER
#
# REPOSITORY ONLY
# - Preserves attempt_count.
# - Preserves payload.
# - Stores terminal failure reason.
# - Clears next_retry_at.
# - Does not call Google.
# ============================================================

def qros_queue_mark_dead_letter(
    delivery_event_key,
    error_message
):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                status = 'DEAD_LETTER',
                updated_at = ?,
                last_error = ?,
                next_retry_at = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                worker_id = NULL        
            WHERE delivery_event_key = ?
            """,
            (
                now,
                str(error_message),
                delivery_event_key
            )
        )

        connection.commit()

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.5-C
# DEAD LETTER QUEUE — CONTROLLED REPLAY
#
# REPOSITORY ONLY
# - Replays DEAD_LETTER events only.
# - Moves event back to PENDING.
# - Resets durable attempt budget.
# - Clears terminal error.
# - Clears retry schedule.
# - Preserves payload and delivery_event_key.
# - Does not call Google.
# ============================================================

def qros_queue_replay_dead_letter(
    delivery_event_key
):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        cursor = connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                status = 'PENDING',
                attempt_count = 0,
                updated_at = ?,
                delivered_at = NULL,
                last_error = NULL,
                next_retry_at = NULL
            WHERE delivery_event_key = ?
              AND status = 'DEAD_LETTER'
            """,
            (
                now,
                delivery_event_key
            )
        )

        connection.commit()

        if cursor.rowcount == 0:
            return {
                "replayed": False,
                "delivery_event_key":
                    delivery_event_key,
                "status": "NOT_DEAD_LETTER"
            }

        return {
            "replayed": True,
            "delivery_event_key":
                delivery_event_key,
            "status": "PENDING"
        }

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.5-D
# DEAD LETTER QUEUE — LIST DEAD LETTER EVENTS
#
# READ ONLY
# - Lists DEAD_LETTER events only.
# - Oldest events first.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

def qros_queue_list_dead_letters(limit=100):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        rows = connection.execute(
            """
            SELECT
                id,
                delivery_event_key,
                trade_id,
                event_phase,
                payload_json,
                status,
                attempt_count,
                created_at,
                updated_at,
                delivered_at,
                last_error,
                next_retry_at
            FROM qros_delivery_queue
            WHERE status = 'DEAD_LETTER'
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                int(limit),
            )
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        connection.close()
           
# ============================================================
# QROS STEP 45E.8-A
# OBSERVABILITY — QUEUE HEALTH SNAPSHOT
#
# READ ONLY
# - Counts queue states.
# - Counts READY events.
# - Counts scheduled retries still waiting.
# - Counts expired PROCESSING leases.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

def qros_queue_health_snapshot():

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        status_rows = connection.execute(
            """
            SELECT
                status,
                COUNT(*) AS count
            FROM qros_delivery_queue
            GROUP BY status
            """
        ).fetchall()

        status_counts = {
            row["status"]: row["count"]
            for row in status_rows
        }

        ready_now = connection.execute(
            """
            SELECT COUNT(*)
            FROM qros_delivery_queue
            WHERE status = 'PENDING'
              AND (
                    next_retry_at IS NULL
                    OR next_retry_at <= ?
              )
            """,
            (
                now,
            )
        ).fetchone()[0]

        retry_waiting = connection.execute(
            """
            SELECT COUNT(*)
            FROM qros_delivery_queue
            WHERE status = 'PENDING'
              AND next_retry_at IS NOT NULL
              AND next_retry_at > ?
            """,
            (
                now,
            )
        ).fetchone()[0]

        expired_lease = connection.execute(
            """
            SELECT COUNT(*)
            FROM qros_delivery_queue
            WHERE status = 'PROCESSING'
              AND lease_until IS NOT NULL
              AND lease_until <= ?
            """,
            (
                now,
            )
        ).fetchone()[0]

        oldest_pending_created_at = connection.execute(
            """
            SELECT MIN(created_at)
            FROM qros_delivery_queue
            WHERE status = 'PENDING'
            """
        ).fetchone()[0]

        oldest_pending_age_seconds = None

        if oldest_pending_created_at is not None:

            try:
                oldest_pending_datetime = datetime.fromisoformat(
                    oldest_pending_created_at.replace(
                        "Z",
                        "+00:00"
                    )
                )

                now_datetime = datetime.fromisoformat(
                    now.replace(
                        "Z",
                        "+00:00"
                    )
                )

                oldest_pending_age_seconds = max(
                    0,
                    int(
                        (
                            now_datetime
                            - oldest_pending_datetime
                        ).total_seconds()
                    )
                )

            except (TypeError, ValueError):

                oldest_pending_age_seconds = None
        
        total = connection.execute(
            """
            SELECT COUNT(*)
            FROM qros_delivery_queue
            """
        ).fetchone()[0]

        worker_thread_alive = bool(
            _qros_queue_worker_thread is not None
            and _qros_queue_worker_thread.is_alive()
        )

        worker_started_age_seconds = None

        if QROS_QUEUE_WORKER_STARTED_AT is not None:

            try:
                worker_started_datetime = datetime.fromisoformat(
                    QROS_QUEUE_WORKER_STARTED_AT.replace(
                        "Z",
                        "+00:00"
                    )
                )

                now_datetime = datetime.fromisoformat(
                    now.replace(
                        "Z",
                        "+00:00"
                    )
                )

                worker_started_age_seconds = max(
                    0,
                    int(
                        (
                            now_datetime
                            - worker_started_datetime
                        ).total_seconds()
                    )
                )

            except (TypeError, ValueError):
                worker_started_age_seconds = None

        worker_last_successful_cycle_age_seconds = None

        if QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT is not None:

            try:
                last_cycle_datetime = datetime.fromisoformat(
                    QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT.replace(
                        "Z",
                        "+00:00"
                    )
                )

                now_datetime = datetime.fromisoformat(
                    now.replace(
                        "Z",
                        "+00:00"
                    )
                )

                worker_last_successful_cycle_age_seconds = max(
                    0,
                    int(
                        (
                            now_datetime
                            - last_cycle_datetime
                        ).total_seconds()
                    )
                )

            except (TypeError, ValueError):
                worker_last_successful_cycle_age_seconds = None

        return {
            "timestamp": now,
            "total": total,
            "pending": status_counts.get(
                "PENDING",
                0
            ),
            "processing": status_counts.get(
                "PROCESSING",
                0
            ),
            "delivered": status_counts.get(
                "DELIVERED",
                0
            ),
            "failed": status_counts.get(
                "FAILED",
                0
            ),
            "dead_letter": status_counts.get(
                "DEAD_LETTER",
                0
            ),
            "ready_now": ready_now,
            "retry_waiting": retry_waiting,
            "expired_lease": expired_lease,
            "oldest_pending_created_at":
                oldest_pending_created_at,
            "oldest_pending_age_seconds":
                oldest_pending_age_seconds,
            "worker_enabled":
                QROS_QUEUE_WORKER_ENABLED,
            "worker_thread_alive":
                worker_thread_alive,
            "worker_started_at":
                QROS_QUEUE_WORKER_STARTED_AT,
            "worker_started_age_seconds":
                worker_started_age_seconds,
            "worker_last_successful_cycle_at":
                QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT,
            "worker_last_successful_cycle_age_seconds":
                worker_last_successful_cycle_age_seconds,
            "worker_last_cycle_status":
                QROS_QUEUE_WORKER_LAST_CYCLE_STATUS,
            "worker_last_error_at":
                QROS_QUEUE_WORKER_LAST_ERROR_AT,
            "worker_last_error":
                QROS_QUEUE_WORKER_LAST_ERROR
        }

    finally:
        connection.close()

# ============================================================
# QROS STEP 45E.11-A
# PRODUCTION GUARDRAILS — QUEUE HEALTH CLASSIFICATION
#
# READ ONLY
# - Classifies queue health from existing snapshot metrics.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

def qros_queue_health_classification():

    snapshot = qros_queue_health_snapshot()

    pending = int(
        snapshot.get(
            "pending",
            0
        )
    )

    processing = int(
        snapshot.get(
            "processing",
            0
        )
    )

    dead_letter = int(
        snapshot.get(
            "dead_letter",
            0
        )
    )

    retry_waiting = int(
        snapshot.get(
            "retry_waiting",
            0
        )
    )

    expired_lease = int(
        snapshot.get(
            "expired_lease",
            0
        )
    )

    oldest_pending_age_seconds = (
        snapshot.get(
            "oldest_pending_age_seconds"
        )
    )

    if oldest_pending_age_seconds is not None:
        oldest_pending_age_seconds = int(
            oldest_pending_age_seconds
        )

    worker_enabled = bool(
        snapshot.get(
            "worker_enabled",
            False
        )
    )

    worker_thread_alive = bool(
        snapshot.get(
            "worker_thread_alive",
            False
        )
    )

    worker_started_age_seconds = snapshot.get(
        "worker_started_age_seconds"
    )

    if worker_started_age_seconds is not None:
        worker_started_age_seconds = int(
            worker_started_age_seconds
        )

    worker_last_successful_cycle_age_seconds = snapshot.get(
        "worker_last_successful_cycle_age_seconds"
    )

    if worker_last_successful_cycle_age_seconds is not None:
        worker_last_successful_cycle_age_seconds = int(
            worker_last_successful_cycle_age_seconds
        )

    reasons = []

    if expired_lease > 0:
        reasons.append(
            "EXPIRED_PROCESSING_LEASE"
        )

    if pending >= QROS_HEALTH_PENDING_CRITICAL:
        reasons.append(
            "HIGH_PENDING_BACKLOG"
        )

    if retry_waiting >= QROS_HEALTH_RETRY_CRITICAL:
        reasons.append(
            "HIGH_RETRY_BACKLOG"
        )

    if dead_letter >= QROS_HEALTH_DEAD_LETTER_HIGH:
        reasons.append(
            "HIGH_DEAD_LETTER_COUNT"
        )

    elif dead_letter >= QROS_HEALTH_DEAD_LETTER_DEGRADED:
        reasons.append(
            "ELEVATED_DEAD_LETTER_COUNT"
        )

    if (
        oldest_pending_age_seconds is not None
        and oldest_pending_age_seconds >= QROS_HEALTH_PENDING_STUCK_SECONDS
    ):
        reasons.append(
            "STUCK_PENDING_EVENT"
        )

    elif (
        oldest_pending_age_seconds is not None
        and oldest_pending_age_seconds >= QROS_HEALTH_PENDING_STALE_SECONDS
    ):
        reasons.append(
            "STALE_PENDING_EVENT"
        )

    worker_liveness_critical = False

    if worker_enabled:

        if not worker_thread_alive:
            reasons.append(
                "WORKER_THREAD_NOT_ALIVE"
            )
            worker_liveness_critical = True

        elif (
            worker_last_successful_cycle_age_seconds is None
            and worker_started_age_seconds is not None
            and worker_started_age_seconds >= QROS_HEALTH_WORKER_HEARTBEAT_STALE_SECONDS
        ):
            reasons.append(
                "WORKER_HEARTBEAT_MISSING"
            )
            worker_liveness_critical = True

        elif (
            worker_last_successful_cycle_age_seconds is not None
            and worker_last_successful_cycle_age_seconds >= QROS_HEALTH_WORKER_HEARTBEAT_STALE_SECONDS
        ):
            reasons.append(
                "WORKER_HEARTBEAT_STALE"
            )
            worker_liveness_critical = True

    if (
        expired_lease > 0
        or pending >= QROS_HEALTH_PENDING_CRITICAL
        or retry_waiting >= QROS_HEALTH_RETRY_CRITICAL
        or worker_liveness_critical
        or (
            oldest_pending_age_seconds is not None
            and oldest_pending_age_seconds >= QROS_HEALTH_PENDING_STUCK_SECONDS
        )
    ):
        health = "CRITICAL"

    elif (
        pending >= QROS_HEALTH_PENDING_DEGRADED
        or retry_waiting > 0
        or processing > 5
        or dead_letter >= QROS_HEALTH_DEAD_LETTER_DEGRADED
        or (
            oldest_pending_age_seconds is not None
            and oldest_pending_age_seconds >= QROS_HEALTH_PENDING_STALE_SECONDS
        )
    ):
        health = "DEGRADED"

    else:
        health = "HEALTHY"

    return {
        "health": health,
        "reasons": reasons,
        "queue": snapshot
    }


# ============================================================
# QROS STEP 45E.8-B
# OBSERVABILITY — ACTIVE QUEUE DIAGNOSTIC SNAPSHOT
#
# READ ONLY
# - Lists PENDING and PROCESSING events only.
# - Exposes operational metadata without full payload_json.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

def qros_queue_diagnostic_snapshot(limit=100):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        rows = connection.execute(
            """
            SELECT
                id,
                delivery_event_key,
                trade_id,
                event_phase,
                status,
                attempt_count,
                created_at,
                updated_at,
                delivered_at,
                last_error,
                next_retry_at,
                claimed_at,
                lease_until,
                worker_id
            FROM qros_delivery_queue
            WHERE status IN (
                'PENDING',
                'PROCESSING'
            )
            ORDER BY id ASC
            LIMIT ?
            """,
            (
                int(limit),
            )
        ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    finally:
        connection.close()
        

# ============================================================
# QROS STEP 45E.4-D
# DURABLE RETRY — SCHEDULE RETRY
#
# REPOSITORY ONLY
# - Keeps attempt_count unchanged.
# - Preserves the failure reason.
# - Moves event back to PENDING.
# - Persists next_retry_at.
# - Does not call Google.
# ============================================================

def qros_queue_schedule_retry(
    delivery_event_key,
    next_retry_at,
    error_message
):

    connection = sqlite3.connect(
        QROS_QUEUE_DB_PATH,
        timeout=30
    )

    try:
        connection.execute(
            "PRAGMA busy_timeout = 30000"
        )

        now = qros_queue_now_iso()

        connection.execute(
            """
            UPDATE qros_delivery_queue
            SET
                status = 'PENDING',
                updated_at = ?,
                next_retry_at = ?,
                last_error = ?
            WHERE delivery_event_key = ?
            """,
            (
                now,
                str(next_retry_at),
                str(error_message),
                delivery_event_key
            )
        )

        connection.commit()

    finally:
        connection.close()


# ============================================================
# QROS STEP 45E.3A
# MANUAL DURABLE QUEUE WORKER
#
# CONTROLLED / ONE EVENT ONLY
# - Reads one PENDING event
# - Delivers through certified STEP45D retry path
# - Marks DELIVERED on SUCCESS or DUPLICATE_ALREADY_ACCEPTED
# - Marks FAILED only on final delivery failure
# - No automatic loop yet
# ============================================================

QROS_QUEUE_WORKER_ID = (
    os.getenv(
        "QROS_QUEUE_WORKER_ID",
        "railway-worker"
    ).strip()
    or "railway-worker"
)

def qros_queue_process_one_pending():

    claim_result = qros_queue_claim_one_ready(
        QROS_QUEUE_WORKER_ID,
        lease_seconds=120
    )

    if claim_result.get("claimed") is not True:
        return {
            "processed": False,
            "status": claim_result.get(
                "status",
                "NO_READY_EVENT"
            )
        }

    row = claim_result["event"]

    delivery_event_key = row[
        "delivery_event_key"
    ]

    try:
        payload = json.loads(
            row["payload_json"]
        )
    except Exception as exc:
        qros_queue_mark_failed(
            delivery_event_key,
            "INVALID_PAYLOAD_JSON:" + str(exc)
        )

        return {
            "processed": True,
            "delivery_event_key":
                delivery_event_key,
            "status": "FAILED",
            "error": str(exc)
        }

    google_payload = dict(payload)
    google_payload[
        "webhook_secret"
    ] = GOOGLE_SCRIPT_SECRET

    qros_queue_mark_attempt(
        delivery_event_key,
        None
    )

    delivery_result = (
        send_to_google_with_retry(
            google_payload
        )
    )

    if delivery_result.get(
        "delivered"
    ) is True:

        qros_queue_mark_delivered(
            delivery_event_key
        )

        return {
            "processed": True,
            "delivery_event_key":
                delivery_event_key,
            "status": "DELIVERED",
            "delivery_result":
                delivery_result
        }

    error_message = str(
        delivery_result.get(
            "error",
            delivery_result.get(
                "status",
                "DELIVERY_FAILED"
            )
        )
    )

    durable_attempt_count = (
        int(row["attempt_count"]) + 1
    )

    next_retry_at = (
        qros_queue_calculate_next_retry_at(
            durable_attempt_count
        )
    )

    if next_retry_at is not None:

        qros_queue_schedule_retry(
            delivery_event_key,
            next_retry_at,
            error_message
        )

        return {
            "processed": True,
            "delivery_event_key":
                delivery_event_key,
            "status": "RETRY_SCHEDULED",
            "attempt_count":
                durable_attempt_count,
            "next_retry_at":
                next_retry_at,
            "delivery_result":
                delivery_result
        }

    qros_queue_mark_dead_letter(
        delivery_event_key,
        error_message
    )

    return {
        "processed": True,
        "delivery_event_key":
            delivery_event_key,
        "status": "DEAD_LETTER",
        "attempt_count":
            durable_attempt_count,
        "delivery_result":
            delivery_result
    }

# ============================================================
# QROS STEP 45E.3C-A
# CONTROLLED DURABLE QUEUE WORKER CYCLE
#
# MANUAL / BOUNDED ONLY
# - Reuses certified qros_queue_process_one_pending()
# - Stops when queue has no PENDING event
# - Stops after max_events
# - No background thread
# - No automatic startup
# ============================================================

def qros_queue_process_pending_cycle(max_events=10):

    max_events = int(max_events)

    if max_events <= 0:
        return {
            "processed_count": 0,
            "status": "INVALID_MAX_EVENTS",
            "results": []
        }

    recovery_result = qros_queue_recover_expired_leases(
        limit=max_events
    )    
    
    results = []

    for _ in range(max_events):

        result = qros_queue_process_one_pending()

        if result.get("processed") is not True:
            break

        results.append(result)

    delivered_count = sum(
        1
        for item in results
        if item.get("status") == "DELIVERED"
    )

    retry_scheduled_count = sum(
        1
        for item in results
        if item.get("status") == "RETRY_SCHEDULED"
    )

    dead_letter_count = sum(
        1
        for item in results
        if item.get("status") == "DEAD_LETTER"
    )

    recovered_count = int(
        recovery_result.get(
            "recovered_count",
            0
        )
    )

    print(
        "QROS_QUEUE_OBSERVABILITY",
        "PROCESSED_COUNT="
        + str(len(results)),
        "DELIVERED_COUNT="
        + str(delivered_count),
        "RETRY_SCHEDULED_COUNT="
        + str(retry_scheduled_count),
        "DEAD_LETTER_COUNT="
        + str(dead_letter_count),
        "RECOVERED_LEASES="
        + str(recovered_count),
        "CYCLE_STATUS="
        + (
            "PROCESSED"
            if results
            else "NO_PENDING_EVENT"
        ),
        flush=True
    )    
    
    return {
        "processed_count": len(results),
        "status": (
            "PROCESSED"
            if results
            else "NO_PENDING_EVENT"
        ),
        "recovery_result": recovery_result,
        "results": results
    }

# ============================================================
# QROS STEP 45E.11-F
# PRODUCTION GUARDRAILS — WORKER LIVENESS HEARTBEAT
#
# IN-MEMORY OBSERVABILITY ONLY
# - Tracks worker start and last successful cycle.
# - Tracks last worker loop error.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

QROS_QUEUE_WORKER_STARTED_AT = None
QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT = None
QROS_QUEUE_WORKER_LAST_CYCLE_STATUS = None
QROS_QUEUE_WORKER_LAST_ERROR_AT = None
QROS_QUEUE_WORKER_LAST_ERROR = None


# ============================================================
# QROS STEP 45E.3C-B
# AUTOMATIC QUEUE WORKER LOOP
#
# CONTROLLED / NOT STARTED HERE
# - Runs bounded queue cycles
# - Sleeps between cycles
# - Controlled by QROS_QUEUE_WORKER_ENABLED
# - No startup hook yet
# ============================================================

def qros_queue_worker_loop():

    global QROS_QUEUE_WORKER_STARTED_AT
    global QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT
    global QROS_QUEUE_WORKER_LAST_CYCLE_STATUS
    global QROS_QUEUE_WORKER_LAST_ERROR_AT
    global QROS_QUEUE_WORKER_LAST_ERROR

    QROS_QUEUE_WORKER_STARTED_AT = qros_queue_now_iso()

    print(
        "QROS_QUEUE_WORKER_LOOP STARTED",
        "INTERVAL_SECONDS="
        + str(QROS_QUEUE_WORKER_INTERVAL_SECONDS),
        "MAX_EVENTS_PER_CYCLE="
        + str(QROS_QUEUE_WORKER_MAX_EVENTS_PER_CYCLE),
        flush=True
    )

    while QROS_QUEUE_WORKER_ENABLED:

        try:

            cycle_result = qros_queue_process_pending_cycle(
                max_events=
                    QROS_QUEUE_WORKER_MAX_EVENTS_PER_CYCLE
            )

            QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT = (
                qros_queue_now_iso()
            )

            QROS_QUEUE_WORKER_LAST_CYCLE_STATUS = str(
                cycle_result.get(
                    "status",
                    ""
                )
            )

            QROS_QUEUE_WORKER_LAST_ERROR_AT = None
            QROS_QUEUE_WORKER_LAST_ERROR = None

            print(
                "QROS_QUEUE_WORKER_CYCLE",
                "PROCESSED_COUNT="
                + str(
                    cycle_result.get(
                        "processed_count",
                        0
                    )
                ),
                "STATUS="
                + QROS_QUEUE_WORKER_LAST_CYCLE_STATUS,
                "HEARTBEAT_AT="
                + QROS_QUEUE_WORKER_LAST_SUCCESSFUL_CYCLE_AT,
                flush=True
            )

        except Exception as exc:

            QROS_QUEUE_WORKER_LAST_ERROR_AT = (
                qros_queue_now_iso()
            )

            QROS_QUEUE_WORKER_LAST_ERROR = str(exc)

            print(
                "QROS_QUEUE_WORKER_ERROR",
                "ERROR=" + str(exc),
                "ERROR_AT="
                + QROS_QUEUE_WORKER_LAST_ERROR_AT,
                flush=True
            )

        time.sleep(
            QROS_QUEUE_WORKER_INTERVAL_SECONDS
        )

    print(
        "QROS_QUEUE_WORKER_LOOP STOPPED",
        flush=True
    )

# ============================================================
# QROS STEP 45E.3C-C
# AUTOMATIC QUEUE WORKER — CONTROLLED STARTER
#
# - Starts only when QROS_QUEUE_WORKER_ENABLED=True
# - Prevents duplicate start inside the same Python process
# - Does NOT start anything by itself
# ============================================================

_qros_queue_worker_thread = None


def qros_queue_start_worker_if_enabled():

    global _qros_queue_worker_thread

    if not QROS_QUEUE_WORKER_ENABLED:

        print(
            "QROS_QUEUE_WORKER_START SKIPPED ENABLED=False",
            flush=True
        )

        return {
            "started": False,
            "status": "DISABLED"
        }

    if (
        _qros_queue_worker_thread is not None
        and _qros_queue_worker_thread.is_alive()
    ):

        print(
            "QROS_QUEUE_WORKER_START SKIPPED ALREADY_RUNNING",
            flush=True
        )

        return {
            "started": False,
            "status": "ALREADY_RUNNING"
        }

    _qros_queue_worker_thread = Thread(
        target=qros_queue_worker_loop,
        daemon=True,
        name="qros-durable-queue-worker"
    )

    _qros_queue_worker_thread.start()

    print(
        "QROS_QUEUE_WORKER_START STARTED",
        flush=True
    )

    return {
        "started": True,
        "status": "STARTED"
    }


# ============================================================
# QROS STEP 45E.9-E
# WEBHOOK HARDENING — JSON ERROR RESPONSES
#
# HTTP ERROR HANDLERS ONLY
# - Keeps API error responses in JSON format.
# - Does not expose internal exception details.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

@app.errorhandler(404)
def qros_http_not_found(error):

    return jsonify({
        "status": "error",
        "guard": "QROS_STEP45E9E",
        "reason": "NOT_FOUND"
    }), 404


@app.errorhandler(405)
def qros_http_method_not_allowed(error):

    return jsonify({
        "status": "error",
        "guard": "QROS_STEP45E9E",
        "reason": "METHOD_NOT_ALLOWED"
    }), 405


@app.errorhandler(500)
def qros_http_internal_error(error):

    print(
        "QROS_WEBHOOK_HARDENING",
        "ERROR=INTERNAL_SERVER_ERROR",
        flush=True
    )

    return jsonify({
        "status": "error",
        "guard": "QROS_STEP45E9E",
        "reason": "INTERNAL_SERVER_ERROR"
    }), 500    

@app.route("/webhook", methods=["POST"])
def webhook():

    
    # ========================================================
    # QROS STEP 45E.9-A
    # REQUEST BODY SIZE GUARD
    # ========================================================

    content_length = request.content_length

    if (
        content_length is not None
        and content_length > QROS_WEBHOOK_MAX_BODY_BYTES
    ):

        print(
            "QROS_WEBHOOK_HARDENING",
            "REJECTED=PAYLOAD_TOO_LARGE",
            "CONTENT_LENGTH="
            + str(content_length),
            "MAX_BYTES="
            + str(QROS_WEBHOOK_MAX_BODY_BYTES),
            flush=True
        )

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45E9A",
            "reason": "PAYLOAD_TOO_LARGE",
            "max_body_bytes":
                QROS_WEBHOOK_MAX_BODY_BYTES
        }), 413

    
    # ========================================================
    # QROS STEP 45E.9-B
    # CONTENT TYPE JSON GUARD
    # ========================================================

    if not request.is_json:

        print(
            "QROS_WEBHOOK_HARDENING",
            "REJECTED=UNSUPPORTED_CONTENT_TYPE",
            "CONTENT_TYPE="
            + str(request.content_type),
            flush=True
        )

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45E9B",
            "reason": "UNSUPPORTED_CONTENT_TYPE",
            "expected_content_type":
                "application/json"
        }), 415
    
    
    # ========================================================
    # STEP 45B.1 — SAFE JSON PARSING
    # ========================================================

    raw_body = request.get_data(
        cache=True,
        as_text=True
    )

    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError as exc:

        print(
            "QROS_WEBHOOK_HARDENING",
            "REJECTED=MALFORMED_JSON",
            "ERROR=" + str(exc),
            flush=True
        )

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45E9C",
            "reason": "MALFORMED_JSON"
        }), 400

    if not isinstance(data, dict):

        print(
            "QROS_PAYLOAD_GUARD "
            "VALID=false "
            "ERRORS=['INVALID_JSON_OR_ROOT']",
            flush=True
        )

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45B1",
            "reason": "INVALID_JSON_OR_ROOT"
        }), 400

    # ========================================================
    # QROS STEP 45E.9-D
    # INGRESS AUTHENTICATION GUARD
    # ========================================================

    ingress_secret = str(
        data.get(
            "qros_ingress_secret",
            ""
        )
    ).strip()

    ingress_secret_configured = bool(
        QROS_WEBHOOK_INGRESS_SECRET
    )

    ingress_secret_valid = (
        ingress_secret_configured
        and bool(ingress_secret)
        and hmac.compare_digest(
            ingress_secret,
            QROS_WEBHOOK_INGRESS_SECRET
        )
    )

    print(
        "QROS_WEBHOOK_AUTH",
        "MODE=" + QROS_WEBHOOK_AUTH_MODE,
        "CONFIGURED="
        + str(ingress_secret_configured),
        "VALID="
        + str(ingress_secret_valid),
        "TRADE_ID="
        + str(
            data.get(
                "trade_id",
                ""
            )
        ),
        flush=True
    )

    if (
        QROS_WEBHOOK_AUTH_MODE == "ENFORCE"
        and not ingress_secret_configured
    ):

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45E9D",
            "reason":
                "INGRESS_SECRET_NOT_CONFIGURED"
        }), 503

    if (
        QROS_WEBHOOK_AUTH_MODE == "ENFORCE"
        and not ingress_secret_valid
    ):

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45E9D",
            "reason":
                "UNAUTHORIZED_WEBHOOK"
        }), 401

    
    # ========================================================
    # STEP 45B.1 — CONTRACT VALIDATION
    # ========================================================

    validation = validate_qros_v49_payload(
        data
    )

    print(
        "QROS_PAYLOAD_GUARD",
        "MODE=" + QROS_PAYLOAD_GUARD_MODE,
        "VALID=" + str(
            validation["valid"]
        ),
        "TRADE_ID=" + str(
            data.get(
                "trade_id",
                ""
            )
        ),
        "RESULT=" + str(
            data.get(
                "result",
                ""
            )
        ),
        "ERRORS=" + str(
            validation["errors"]
        ),
        "WARNINGS=" + str(
            validation["warnings"]
        ),
        flush=True
    )

    # ========================================================
    # ENFORCE MODE
    #
    # IMPORTANT:
    # During STEP 45B.1 we remain SHADOW.
    # This branch will therefore NOT run yet.
    # ========================================================

    if (
        QROS_PAYLOAD_GUARD_MODE
        == "ENFORCE"
        and not validation["valid"]
    ):

        return jsonify({
            "status": "rejected",
            "guard": "QROS_STEP45B1",
            "errors":
                validation["errors"],
            "warnings":
                validation["warnings"]
        }), 422

    # ========================================================
    # EXISTING DELIVERY PATH
    #
    # Preserved unchanged for STEP 45B.
    # ========================================================

    # ========================================================
    # QROS STEP 45E.2C
    # DURABLE QUEUE — SHADOW ENQUEUE
    #
    # IMPORTANT:
    # - Persist event before legacy background delivery.
    # - Existing STEP45D Google delivery remains unchanged.
    # - Queue does NOT deliver anything yet.
    # ========================================================

    try:

        queue_result = qros_queue_enqueue(
            data
        )

        print(
            "QROS_STEP45E2C_SHADOW_ENQUEUE",
            "TRADE_ID=" + str(
                data.get("trade_id", "")
            ),
            "RESULT=" + str(
                data.get("result")
                or data.get("resultat")
                or ""
            ),
            "ENQUEUED=" + str(
                queue_result.get(
                    "enqueued",
                    False
                )
            ),
            "DUPLICATE=" + str(
                queue_result.get(
                    "duplicate",
                    False
                )
            ),
            "STATUS=" + str(
                queue_result.get(
                    "status",
                    ""
                )
            ),
            "KEY=" + str(
                queue_result.get(
                    "delivery_event_key",
                    ""
                )
            ),
            flush=True
        )

    except Exception as queue_error:

        # STEP45E.2C remains SHADOW.
        # A queue failure must NOT break the certified
        # STEP45D delivery path during this phase.

        print(
            "QROS_STEP45E2C_SHADOW_ERROR",
            "TRADE_ID=" + str(
                data.get("trade_id", "")
            ),
            "ERROR=" + str(
                queue_error
            ),
            flush=True
        )
        
    # ========================================================
    # QROS STEP 45E.10-B
    # CONTROLLED CUTOVER — DELIVERY PATH ROUTING
    #
    # LEGACY:
    # - Preserves certified background delivery.
    #
    # DURABLE_QUEUE:
    # - Does NOT start legacy background delivery.
    # - Event remains persisted in durable queue.
    #
    # No automatic mode change.
    # ========================================================

    if QROS_DELIVERY_PATH_MODE == "LEGACY":

        print(
            "QROS_DELIVERY_PATH",
            "MODE=LEGACY",
            "ACTION=START_LEGACY_BACKGROUND",
            "TRADE_ID="
            + str(
                data.get(
                    "trade_id",
                    ""
                )
            ),
            flush=True
        )

        Thread(
            target=process_webhook_background,
            args=(data,),
            daemon=True
        ).start()

    elif QROS_DELIVERY_PATH_MODE == "DURABLE_QUEUE":

        print(
            "QROS_DELIVERY_PATH",
            "MODE=DURABLE_QUEUE",
            "ACTION=QUEUE_ONLY",
            "TRADE_ID="
            + str(
                data.get(
                    "trade_id",
                    ""
                )
            ),
            flush=True
        )

    else:

        print(
            "QROS_DELIVERY_PATH",
            "MODE="
            + str(QROS_DELIVERY_PATH_MODE),
            "ACTION=INVALID_MODE",
            flush=True
        )

        return jsonify({
            "status": "error",
            "guard": "QROS_STEP45E10B",
            "reason": "INVALID_DELIVERY_PATH_MODE"
        }), 503

    return jsonify({
        
        "status": "received",
        "message":
            "Webhook received by Railway",

        "payload_guard": {
            "mode":
                QROS_PAYLOAD_GUARD_MODE,

            "valid":
                validation["valid"],

            "errors":
                validation["errors"],

            "warnings":
                validation["warnings"]
        }
    }), 200

# ============================================================
# QROS STEP 45B.2
# Controlled Payload Validation Endpoint
#
# READ ONLY
# - No CSV write
# - No Google Apps Script call
# - No QROS mutation
# ============================================================

@app.route("/validate", methods=["POST"])
def validate_payload_only():

    data = request.get_json(
        silent=True
    )

    if not isinstance(data, dict):

        return jsonify({
            "status": "INVALID",
            "guard": "QROS_STEP45B2",
            "valid": False,
            "errors": [
                "INVALID_JSON_OR_ROOT"
            ],
            "warnings": []
        }), 400

    validation = validate_qros_v49_payload(
        data
    )

    print(
        "QROS_STEP45B2_VALIDATE",
        "VALID=" + str(
            validation["valid"]
        ),
        "TRADE_ID=" + str(
            data.get("trade_id", "")
        ),
        "RESULT=" + str(
            data.get("result", "")
        ),
        "ERRORS=" + str(
            validation["errors"]
        ),
        "WARNINGS=" + str(
            validation["warnings"]
        ),
        flush=True
    )

    return jsonify({
        "status":
            "VALID"
            if validation["valid"]
            else "INVALID",

        "guard":
            "QROS_STEP45B2",

        "valid":
            validation["valid"],

        "errors":
            validation["errors"],

        "warnings":
            validation["warnings"]
    }), (
        200
        if validation["valid"]
        else 422
    )

# ============================================================
# QROS STEP 45E.8-D
# OBSERVABILITY — READ ONLY HEALTH ENDPOINT
#
# READ ONLY
# - Exposes queue health snapshot over HTTP.
# - Does not modify queue state.
# - Does not call Google.
# ============================================================

@app.route("/qros/health", methods=["GET"])
def qros_health_endpoint():

    classification = qros_queue_health_classification()

    return jsonify({
        "status": "OK",
        "health": classification["health"],
        "reasons": classification["reasons"],
        "queue": classification["queue"]
    }), 200

def send_to_google_with_retry(google_payload, max_attempts=3):

    retry_delays = [2, 5]
    last_error = None

    for attempt in range(1, max_attempts + 1):

        try:

            print(
                f"QROS_DELIVERY ATTEMPT={attempt}/{max_attempts}",
                flush=True
            )
                
            response = requests.post(
                GOOGLE_SCRIPT_URL,
                json=google_payload,
                timeout=(10, 90)
            )

            print(
                f"QROS_DELIVERY HTTP_STATUS={response.status_code}",
                flush=True
            )

            print(
                f"QROS_DELIVERY RESPONSE={response.text}",
                flush=True
            )

            if 200 <= response.status_code < 300:

                try:
                    response_payload = response.json()
                except ValueError:
                    response_payload = {}

                qros_status = str(
                    response_payload.get("status", "")
                ).strip()

                if qros_status in (
                    "SUCCESS",
                    "DUPLICATE_ALREADY_ACCEPTED"
                ):

                    print(
                        "QROS_DELIVERY CONFIRMED "
                        f"ATTEMPT={attempt} "
                        f"STATUS={qros_status}",
                        flush=True
                    )

                    return {
                        "delivered": True,
                        "attempts": attempt,
                        "status": qros_status,
                        "http_status": response.status_code
                    }

                last_error = (
                    "UNEXPECTED_QROS_ACK:"
                    + qros_status
                )

                print(
                    "QROS_DELIVERY UNEXPECTED_ACK "
                    f"ATTEMPT={attempt} "
                    f"STATUS={qros_status}",
                    flush=True
                )

            elif response.status_code >= 500:

                last_error = (
                    "GOOGLE_HTTP_"
                    + str(response.status_code)
                )

            else:

                print(
                    "QROS_DELIVERY PERMANENT_HTTP_FAILURE "
                    f"STATUS={response.status_code}",
                    flush=True
                )

                return {
                    "delivered": False,
                    "attempts": attempt,
                    "status": "PERMANENT_HTTP_FAILURE",
                    "http_status": response.status_code,
                    "error": response.text
                }

        except requests.Timeout as exc:

            last_error = (
                "TIMEOUT:"
                + str(exc)
            )

            print(
                "QROS_DELIVERY TIMEOUT "
                f"ATTEMPT={attempt}",
                flush=True
            )

        except requests.RequestException as exc:

            last_error = (
                "REQUEST_ERROR:"
                + str(exc)
            )

            print(
                "QROS_DELIVERY REQUEST_ERROR "
                f"ATTEMPT={attempt} "
                f"ERROR={exc}",
                flush=True
            )

        if attempt < max_attempts:

            delay = retry_delays[
                attempt - 1
            ]

            print(
                "QROS_DELIVERY RETRY_SCHEDULED "
                f"IN={delay}s",
                flush=True
            )

            time.sleep(delay)

    print(
        "QROS_DELIVERY FAILED "
        f"ATTEMPTS={max_attempts} "
        f"LAST_ERROR={last_error}",
        flush=True
    )

    return {
        "delivered": False,
        "attempts": max_attempts,
        "status": "DELIVERY_FAILED",
        "error": last_error
    }
    
def process_webhook_background(data):
    try:
        if not GOOGLE_SCRIPT_SECRET:
            raise RuntimeError(
                "GOOGLE_SCRIPT_SECRET manquant dans Railway Variables"
            )
        
        trade = {
            "trade_id": data.get("trade_id", ""),            
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": data.get("symbol", ""),
            "timeframe": data.get("timeframe", ""),
            "direction": data.get("direction", ""),
            "action": data.get("action", ""),
            "mode": data.get("mode", ""),
            "setup": data.get("setup", ""),
            "score": data.get("score", 0),
            "session": data.get("session", ""),
            "entry": data.get("entry", 0),
            "sl": data.get("sl", 0),
            "tp": data.get("tp", 0),
            "rr": data.get("rr", 0),
            "htf_trend": data.get("htf_trend", ""),
            "bos": data.get("bos", False),
            "fvg": data.get("fvg", False),
            "sweep": data.get("sweep", False),

            "version": data.get("version", ""),
            "fingerprint": data.get("fingerprint", ""),
            "cis": data.get("cis", ""),
            "cis_score": data.get("cis_score", ""),
            "context_class": data.get("context_class", ""),
            "learning_tag": data.get("learning_tag", ""),
            "institutional_ai_label": data.get("institutional_ai_label", ""),
            "institutional_ai_score": data.get("institutional_ai_score", ""),
            "institutional_confidence_label": data.get("institutional_confidence_label", ""),
            "institutional_confidence_score": data.get("institutional_confidence_score", ""),
            "institutional_rank": data.get("institutional_rank", ""),
            "institutional_rank_score": data.get("institutional_rank_score", ""),
            "institutional_pattern": data.get("institutional_pattern", ""),
            "institutional_pattern_quality": data.get("institutional_pattern_quality", ""),
            "institutional_outcome_key": data.get("institutional_outcome_key", ""),
            "institutional_outcome_group": data.get("institutional_outcome_group", ""),
            "context_scorecard": data.get("context_scorecard", ""),
            "context_scorecard_value": data.get("context_scorecard_value", ""),

            "tvs": data.get("tvs", ""),
            "tvs_score": data.get("tvs_score", ""),
            "penalty": data.get("penalty", ""),
            "penalty_score": data.get("penalty_score", ""),
            "exit_mode": data.get("exit_mode", ""),
            "management_mode": data.get("management_mode", ""),
            "be_state": data.get("be_state", ""),
            "thesis": data.get("thesis", ""),

            "setup_id": data.get("setup_id", ""),
            "setup_family": data.get("setup_family", ""),
            "learning_bucket": data.get("learning_bucket", ""),
            "learning_bucket_score": data.get("learning_bucket_score", ""),
            "historical_context_group": data.get("historical_context_group", ""),
            "historical_context_quality": data.get("historical_context_quality", ""),
            "edge_stability": data.get("edge_stability", ""),
            "edge_stability_score": data.get("edge_stability_score", ""),

            "v49_memory_score": data.get("v49_memory_score"),
            "v49_memory_quality": data.get("v49_memory_quality"),
            "v49_memory_bias": data.get("v49_memory_bias"),
            "v49_memory_fingerprint": data.get("v49_memory_fingerprint"),
            "v49_liquidity_memory_bias": data.get("v49_liquidity_memory_bias"),
            "v49_liquidity_memory_score": data.get("v49_liquidity_memory_score"),
            "v49_fvg_bias": data.get("v49_fvg_bias"),
            "v49_fvg_memory_score": data.get("v49_fvg_memory_score"),
            "v49_htf_level_memory": data.get("v49_htf_level_memory"),
            "v49_htf_level_score": data.get("v49_htf_level_score"),

            "dominant_scenario": data.get("dominant_scenario", ""),
            "scenario_confidence": data.get("scenario_confidence", ""),
            "bull_scenario": data.get("bull_scenario", ""),
            "bull_confidence": data.get("bull_confidence", ""),
            "bear_scenario": data.get("bear_scenario", ""),
            "bear_confidence": data.get("bear_confidence", ""),

            "narrative_phase": data.get("narrative_phase", ""),
            "narrative_score": data.get("narrative_score", ""),
            "narrative_quality": data.get("narrative_quality", ""),
            "narrative_text": data.get("narrative_text", ""),
            "alignment": data.get("alignment", ""),
            "structure": data.get("structure", ""),
            "structure_strength": data.get("structure_strength", ""),
            "trend_persistence": data.get("trend_persistence", ""),
            "choch": data.get("choch", ""),
            "choch_score": data.get("choch_score", ""),
            "consensus": data.get("consensus", ""),
            "consensus_score": data.get("consensus_score", ""),
            "cycle": data.get("cycle", ""),
            "hierarchy_alignment": data.get("hierarchy_alignment", ""),
            "sync_state": data.get("sync_state", ""),
            "context_score": data.get("context_score", ""),
            "environment": data.get("environment", ""),

            "regime": data.get("regime", ""),
            "regime_quality": data.get("regime_quality", ""),
            "regime_score": data.get("regime_score", ""),

            "resultat": data.get("result") or data.get("resultat") or "OPEN"
        }

        colonnes = [
            "trade_id", "date", "symbol", "timeframe", "direction", "action", "mode", "setup",
            "score", "session", "entry", "sl", "tp", "rr", "htf_trend", "bos",
            "fvg", "sweep", "version", "fingerprint", "cis", "cis_score",
            "context_class", "learning_tag", "institutional_ai_label",
            "institutional_ai_score", "institutional_confidence_label",
            "institutional_confidence_score", "dominant_scenario",
            "institutional_rank", "institutional_rank_score", "institutional_pattern",
            "institutional_pattern_quality", "institutional_outcome_key",
            "institutional_outcome_group", "context_scorecard",
            "context_scorecard_value", "tvs", "tvs_score", "penalty",
            "penalty_score", "exit_mode", "management_mode", "be_state", "thesis",
            "setup_id", "setup_family", "learning_bucket", "learning_bucket_score",
            "historical_context_group", "historical_context_quality",
            "edge_stability", "edge_stability_score", "scenario_confidence",
            "bull_scenario", "bull_confidence", "bear_scenario", "bear_confidence",
            "narrative_phase", "narrative_score", "narrative_quality",
            "narrative_text", "alignment", "structure", "structure_strength",
            "trend_persistence", "choch", "choch_score", "consensus",
            "consensus_score", "cycle", "hierarchy_alignment", "sync_state",
            "context_score", "environment", "tvs", "tvs_score", "penalty",
            "penalty_score", "exit_mode", "management_mode", "be_state", "thesis",
            "regime", "regime_quality", "regime_score", "v49_memory_score",
            "v49_memory_quality", "v49_memory_bias", "v49_memory_fingerprint",
            "v49_liquidity_memory_bias", "v49_liquidity_memory_score",
            "v49_fvg_bias", "v49_fvg_memory_score", "v49_htf_level_memory",
            "v49_htf_level_score", "resultat"
        ]

        with open(fichier, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=colonnes)

            if file.tell() == 0:
                writer.writeheader()

            writer.writerow(trade)

        google_payload = dict(trade)

        google_payload["webhook_secret"] = GOOGLE_SCRIPT_SECRET

        delivery_result = send_to_google_with_retry(
            google_payload
        )

        print(
            "Trade reçu :",
            trade,
            flush=True
        )

        print(
            "QROS DELIVERY RESULT =",
            delivery_result,
            flush=True
        )    
    except Exception as e:
        print(
            "BACKGROUND ERROR =", 
            str(e), 
            flush=True
        )

@app.route("/download")
def download_csv():
    return send_file("journal_trades.csv", as_attachment=True)

# ============================================================
# QROS STEP 45E.3C-D
# AUTOMATIC QUEUE WORKER — APPLICATION STARTUP
#
# Starts only if QROS_QUEUE_WORKER_ENABLED=True.
# Default remains OFF.
# ============================================================

qros_queue_start_worker_if_enabled()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8080))
    )
