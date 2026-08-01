"""
test_atlas.py
=============
Diagnostic tool for "SSL handshake failed / tlsv1 alert internal error /
ReplicaSetNoPrimary" against MongoDB Atlas.

It NEVER hardcodes credentials -- it reads MONGO_URI from the environment.
Set it first:

    Windows (cmd):   set MONGO_URI=mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?appName=Cluster0
    Windows (PS):    $env:MONGO_URI="mongodb+srv://user:pass@cluster0.xxxxx.mongodb.net/?appName=Cluster0"

Run:
    py -3.11 test_atlas.py

What it does, in order, to isolate WHERE the failure is:
  1. Resolves the Atlas SRV/TXT DNS records for the cluster hostname.
     If this fails -> DNS/network problem, not a pymongo/TLS problem.
  2. Opens a raw TCP+TLS socket (no pymongo) to one resolved host on
     port 27017 and reports the exact TLS alert, if any. If a raw socket
     also gets "tlsv1 alert internal error", the problem is 100% outside
     your Python code -- it's the network path (antivirus SSL
     inspection, campus/hostel firewall, corporate proxy) or the Atlas
     cluster itself, and no pymongo option can fix it.
  3. Tries pymongo with three progressively more permissive TLS configs,
     so you can tell a certificate problem apart from a handshake/
     network problem:
       - certifi CA bundle (recommended, production-safe)
       - tlsAllowInvalidCertificates=True (rules out cert-chain issues)
       - tlsInsecure=True (rules out hostname-verification issues)
     If ALL THREE fail with the same low-level alert, the fix is not in
     this file -- see the network checklist printed at the end.
"""

import os
import socket
import ssl
import sys
from urllib.parse import urlparse

import certifi

MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set the MONGO_URI environment variable first (see docstring).")


def step1_resolve_srv():
    print("\n[1/3] Resolving Atlas SRV/TXT DNS records...")
    if not MONGO_URI.startswith("mongodb+srv://"):
        print("      Not an SRV URI -- skipping (standard mongodb:// URI).")
        return None
    host = urlparse(MONGO_URI.replace("mongodb+srv://", "https://")).hostname
    try:
        import dns.resolver  # provided by dnspython, a pymongo[srv] dependency
        srv_records = dns.resolver.resolve(f"_mongodb._tcp.{host}", "SRV")
        hosts = [str(r.target).rstrip(".") for r in srv_records]
        print(f"      OK -- {len(hosts)} host(s) resolved: {hosts}")
        return hosts[0] if hosts else None
    except Exception as e:
        print(f"      FAILED: {e}")
        print("      -> This is a DNS problem, not a pymongo/TLS problem.")
        print("         Try a different DNS resolver (e.g. 8.8.8.8) or a "
              "different network.")
        return None


def step2_raw_tls_handshake(host):
    print("\n[2/3] Raw TLS handshake test (bypasses pymongo entirely)...")
    if not host:
        print("      Skipped -- no host from step 1.")
        return
    ctx = ssl.create_default_context(cafile=certifi.where())
    try:
        with socket.create_connection((host, 27017), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as tls_sock:
                print(f"      OK -- TLS handshake succeeded "
                      f"({tls_sock.version()}, {tls_sock.cipher()[0]})")
    except ssl.SSLError as e:
        print(f"      FAILED (SSL layer): {e}")
        print("      -> pymongo cannot fix this. The TLS handshake itself is")
        print("         being rejected or intercepted before MongoDB's code")
        print("         even runs. Most likely causes, in order:")
        print("           * Antivirus/endpoint-security SSL inspection")
        print("             (Kaspersky, McAfee, Bitdefender, etc.) -- try")
        print("             temporarily disabling HTTPS/SSL scanning.")
        print("           * A campus/hostel/corporate network blocking or")
        print("             intercepting port 27017 -- try a mobile hotspot.")
        print("           * System clock significantly wrong -- check date/time.")
    except OSError as e:
        print(f"      FAILED (network/socket layer): {e}")
        print("      -> Could not even open a TCP connection on port 27017.")
        print("         Check Atlas Network Access (IP allow-list) and any")
        print("         local firewall blocking outbound 27017.")


def step3_pymongo_configs():
    print("\n[3/3] pymongo connection attempts (progressively more permissive)...")
    import pymongo

    configs = [
        ("certifi CA bundle (production-safe)",
         {"tls": True, "tlsCAFile": certifi.where()}),
        ("tlsAllowInvalidCertificates=True (rules out cert-chain issues)",
         {"tls": True, "tlsAllowInvalidCertificates": True}),
        ("tlsInsecure=True (rules out hostname-verification issues)",
         {"tls": True, "tlsInsecure": True}),
    ]

    for name, opts in configs:
        print(f"\n  Trying: {name}")
        try:
            client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000, **opts)
            dbs = client.list_database_names()
            print(f"  SUCCESS -- databases: {dbs}")
            client.close()
            return True
        except Exception as e:
            print(f"  FAILED: {str(e)[:200]}")
    return False


if __name__ == "__main__":
    host = step1_resolve_srv()
    step2_raw_tls_handshake(host)
    ok = step3_pymongo_configs()

    print("\n" + "=" * 65)
    if ok:
        print("At least one pymongo config worked -- use that TLS config in")
        print("common/db.py (see MONGO_TLS_INSECURE for the debugging-only flag).")
    else:
        print("All configs failed identically -> this is not a pymongo/code")
        print("problem. Checklist:")
        print("  1. Atlas > Network Access: is 0.0.0.0/0 (or your current IP)")
        print("     in the allow-list?")
        print("  2. Atlas > Database Access: does the user/password in your")
        print("     MONGO_URI still exist and have the right role?")
        print("  3. Try the exact same MONGO_URI from a mobile hotspot --")
        print("     if it works there, your campus/office network or")
        print("     antivirus is intercepting/blocking the connection.")
        print("  4. Check your system clock is correct.")
        print("  5. pip install --upgrade pymongo dnspython certifi")
    print("=" * 65)