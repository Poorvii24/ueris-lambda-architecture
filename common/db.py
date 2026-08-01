"""
common/db.py
=============
Single, centralized MongoDB connection utility for UERIS.

Every layer (batch, speed, serving, streaming, ai, and the one-off
maintenance scripts at the project root) MUST get its MongoClient from
this module instead of calling pymongo.MongoClient(...) directly.

Why this file exists
---------------------
Before this refactor, 12+ call sites across the project each built their
own MongoClient with slightly different (and sometimes contradictory)
TLS options:

    serving_layer/app.py        tlsCAFile=certifi.where(), tlsAllowInvalidCertificates=True
    batch_layer/batch_processing.py   no TLS options at all
    batch_layer/ai_batch_processor.py no TLS options at all
    streaming/consumer.py             no TLS options at all
    speed_layer/*.py                  no TLS options at all
    migrate_to_atlas.py / drop_database.py / etc.   tlsCAFile only

serving_layer/app.py also opened and closed a *brand-new* MongoClient
(i.e. a brand-new TLS handshake) on every single incoming HTTP request,
instead of reusing one pooled connection. Each of those was a fresh
opportunity for a flaky network path to fail the handshake, which is
consistent with intermittent "SSL handshake failed" errors that don't
reproduce the same way twice.

This module fixes both problems:
  1. One TLS configuration, defined once, used everywhere.
  2. One process-wide pooled MongoClient (pymongo.MongoClient is
     thread-safe and is *designed* to be created once and reused --
     creating a new one per request is an anti-pattern).

Environment variables
----------------------
    MONGO_URI                  Connection string (default: mongodb://localhost:27017/)
    DB_NAME                    Database name (default: urban_env_db)
    MONGO_SERVER_SELECTION_MS  Server selection timeout, ms (default: 8000)
    MONGO_CONNECT_TIMEOUT_MS   TCP connect timeout, ms (default: 8000)
    MONGO_SOCKET_TIMEOUT_MS    Socket read/write timeout, ms (default: 20000)
    MONGO_TLS_INSECURE         "true" to skip certificate verification.
                               NEVER set this in production/Render -- it
                               exists only so you can bisect "is this a
                               certificate problem or a network problem"
                               while debugging locally.
"""

import os
import threading

import certifi
import pymongo
from pymongo.errors import ConfigurationError

_lock = threading.Lock()
_client: pymongo.MongoClient | None = None
_client_uri: str | None = None


def _is_true(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def build_client(uri: str, **overrides) -> pymongo.MongoClient:
    """
    Build a new MongoClient for `uri` with UERIS's standard, consistent
    TLS/timeout configuration. Use this directly (instead of get_client())
    only in standalone scripts that need to talk to more than one URI at
    once, e.g. migrate_to_atlas.py connecting to a local DB *and* Atlas
    in the same process.
    """
    is_srv = uri.startswith("mongodb+srv://")
    insecure = _is_true(os.environ.get("MONGO_TLS_INSECURE"))

    kwargs = dict(
        serverSelectionTimeoutMS=int(os.environ.get("MONGO_SERVER_SELECTION_MS", 8000)),
        connectTimeoutMS=int(os.environ.get("MONGO_CONNECT_TIMEOUT_MS", 8000)),
        socketTimeoutMS=int(os.environ.get("MONGO_SOCKET_TIMEOUT_MS", 20000)),
        retryWrites=True,
        appName="ueris",
    )

    if is_srv:
        # Atlas always terminates TLS. Be explicit rather than relying on
        # pymongo's implicit "tls=True because the scheme is +srv" default,
        # and always hand it certifi's CA bundle rather than whatever (or
        # nothing) the OS trust store happens to contain.
        kwargs["tls"] = True
        kwargs["tlsCAFile"] = certifi.where()
        if insecure:
            # Local debugging only -- e.g. to confirm/rule out a
            # certificate-chain problem vs. a network-level TLS problem.
            # Must never be set on Render/production.
            kwargs["tlsAllowInvalidCertificates"] = True

    kwargs.update(overrides)

    try:
        return pymongo.MongoClient(uri, **kwargs)
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"Invalid MONGO_URI / MongoClient configuration: {exc}. "
            "Check for a malformed SRV string, stray whitespace, or an "
            "unsupported combination of TLS options."
        ) from exc


def get_client() -> pymongo.MongoClient:
    """
    Return the process-wide singleton MongoClient, building it on first
    use and rebuilding it if MONGO_URI changes at runtime (tests). Every
    layer should call this (or get_db()) rather than instantiating its
    own MongoClient.
    """
    global _client, _client_uri
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")

    if _client is not None and uri == _client_uri:
        return _client

    with _lock:
        if _client is None or uri != _client_uri:
            if _client is not None:
                _client.close()
            _client = build_client(uri)
            _client_uri = uri
    return _client


def get_db(db_name: str | None = None):
    """Return the configured Database handle from the shared client."""
    name = db_name or os.environ.get("DB_NAME", "urban_env_db")
    return get_client()[name]


def ping() -> bool:
    """Cheap connectivity check for startup banners / /api/health."""
    try:
        get_client().admin.command("ping")
        return True
    except Exception:
        return False


def close() -> None:
    """Close the shared client. Call on graceful process shutdown only."""
    global _client, _client_uri
    with _lock:
        if _client is not None:
            _client.close()
            _client = None
            _client_uri = None
