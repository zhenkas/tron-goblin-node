#!/usr/bin/env python3
"""Loopback wallet HTTP gateway. Runtime JSON configuration stays outside Git.

Run: python3 wallet_gateway.py --config /absolute/path/wallet-gateway.json
Defaults: 127.0.0.1:19180 -> 127.0.0.1:19120. This does not provide TLS.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, fields
from http import client, server
import ipaddress
import json
import os
import stat
import time
import re
from pathlib import Path
import socket
import threading
from urllib.parse import urlsplit

# Optional operator notifications: a `critical_notify.py` module placed beside
# this script and exposing report_fatal(code, summary, error) is used when present.
try:
    from critical_notify import report_fatal
except ModuleNotFoundError as error:
    if error.name != "critical_notify":
        raise

    def report_fatal(code, summary, error):
        return False

CHROME_ORIGIN = "chrome-extension://ibnejdfjmmkpcnlpebklmnkoeoihofec"
BOTH = frozenset({"GET", "POST"})
POST = frozenset({"POST"})
# These are wallet reads, unsigned builders, and signed-transaction broadcasts.
# Never expose generic JSON-RPC, private keys, admin/debug, or index/archive APIs.
ENDPOINTS = {
    "/wallet/getblock": POST,
    "/walletsolidity/getblock": POST,
    **{"/wallet/" + name: BOTH for name in (
        "getnowblock", "getchainparameters", "getnodeinfo", "listwitnesses",
        "getenergyprices", "getbandwidthprices", "getmemofee",
    )},
    **{"/wallet/" + name: POST for name in (
        "getblockbynum", "getblockbyid", "getaccount", "getaccountresource",
        "getaccountnet", "getcontract", "getcontractinfo", "gettransactionbyid",
        "gettransactioninfobyid", "validateaddress", "getassetissuebyid",
        "getassetissuebyname", "getpaginatedassetissuelist",
        "getdelegatedresource", "getdelegatedresourcev2",
        "getdelegatedresourceaccountindex", "getdelegatedresourceaccountindexv2",
        "getcandelegatedmaxsize", "getavailableunfreezecount",
        "getcanwithdrawunfreezeamount", "createtransaction", "transferasset",
        "triggersmartcontract", "triggerconstantcontract", "estimateenergy",
        "freezebalancev2", "unfreezebalancev2", "withdrawexpireunfreeze",
        "delegateresource", "undelegateresource", "cancelallunfreezev2",
        "getsignweight", "broadcasttransaction", "broadcasthex",
    )},
    "/walletsolidity/getnowblock": BOTH,
    **{"/walletsolidity/" + name: POST for name in (
        "getblockbynum", "getaccount", "getaccountresource", "gettransactionbyid",
        "gettransactioninfobyid", "getdelegatedresource", "getdelegatedresourcev2",
    )},
}



GETBLOCK_ROUTES = frozenset({"/wallet/getblock", "/walletsolidity/getblock"})

# Read-only compatibility, never an on-chain contract or generic VM emulation.
MULTICALL_ADDRESS = "41e777b7157d44d95ef48a8483c743a5d3f8a72180"
MULTICALL_SIGNATURE = "walletTokensBalance(address[],address)"
MULTICALL_MAX_TOKENS = 30  # Original TronLink 4.11.2 batchSize.
ADDRESS_HEX = re.compile(r"41[0-9a-fA-F]{40}\Z")
WORD_HEX = re.compile(r"[0-9a-fA-F]*\Z")


class MulticallError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result
    def invalid_constant(_value):
        raise ValueError("invalid JSON constant")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)


def multicall_request(body):
    # Inspect only the exact stock target+method. Other calls retain their bytes
    # and ordinary upstream behavior, including with the feature enabled.
    try:
        value = json.loads(body)
    except (ValueError, TypeError, UnicodeError):
        return None
    if (not isinstance(value, dict)
            or not isinstance(value.get("contract_address"), str)
            or value["contract_address"].lower() != MULTICALL_ADDRESS
            or value.get("function_selector") != MULTICALL_SIGNATURE):
        return None
    try:
        value = strict_json(body)
        expected = {"contract_address", "owner_address", "function_selector", "parameter", "call_value"}
        if (set(value) != expected or type(value["call_value"]) is not int or value["call_value"] != 0
                or not isinstance(value["owner_address"], str)
                or not ADDRESS_HEX.fullmatch(value["owner_address"])):
            raise ValueError("unsupported stock call shape")
        parameter = value["parameter"]
        if (not isinstance(parameter, str) or len(parameter) < 192
                or len(parameter) > (96 + 32 * MULTICALL_MAX_TOKENS) * 2
                or not WORD_HEX.fullmatch(parameter)):
            raise ValueError("invalid ABI bounds")
        words = [parameter[i:i + 64] for i in range(0, len(parameter), 64)]
        if len(parameter) % 64 or int(words[0], 16) != 64 or words[1][:24] != "0" * 24:
            raise ValueError("noncanonical ABI head")
        count = int(words[2], 16)
        if count > MULTICALL_MAX_TOKENS or len(words) != 3 + count:
            raise ValueError("noncanonical ABI array")
        if any(word[:24] != "0" * 24 for word in words[3:]):
            raise ValueError("noncanonical ABI address")
        wallet = words[1].lower()
        tokens = ["41" + word[24:].lower() for word in words[3:]]
        return value["owner_address"].lower(), wallet, tokens
    except (ValueError, TypeError, KeyError, IndexError) as error:
        raise MulticallError(400, "invalid stock Multicall request") from error


def private_multicall_profile(filename):
    try:
        # Trusted installation input, never an HTTP parameter. Read fresh for
        # every request so a regenerated private stand does not need a source pin.
        fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as file:
            meta = os.fstat(file.fileno())
            if (not stat.S_ISREG(meta.st_mode) or meta.st_mode & 0o022
                    or meta.st_uid not in (0, os.geteuid()) or meta.st_size > 4096):
                raise ValueError("unsafe profile file")
            value = strict_json(file.read(4097))
        if (not isinstance(value, dict)
                or set(value) - {"schema", "genesis", "profile_hash", "native_unfreeze_delay_secs"}
                or type(value.get("schema")) is not int or value["schema"] != 1):
            raise ValueError("invalid profile schema")
        genesis, profile = value.get("genesis"), value.get("profile_hash")
        if (not isinstance(genesis, str) or not re.fullmatch(r"0000000000000000[0-9a-f]{48}", genesis)
                or not any(c != "0" for c in genesis[16:])
                or any(genesis.endswith(suffix) for suffix in ("2b6653dc", "94a9059e", "cd8690dc"))
                or not isinstance(profile, str) or not re.fullmatch(r"[0-9a-f]{64}", profile)):
            raise ValueError("invalid private profile identity")
        if "native_unfreeze_delay_secs" in value:
            delay = value["native_unfreeze_delay_secs"]
            if type(delay) is not int or not 60 <= delay <= 86400:
                raise ValueError("invalid private timing marker")
        return genesis, profile
    except (OSError, ValueError, TypeError) as error:
        raise MulticallError(503, "private Multicall profile is unavailable") from error


def multicall_checkpoint(value):
    try:
        raw = value["block_header"]["raw_data"]
        number, block_id = raw.get("number", 0), value["blockID"]
        if (type(number) is not int or number < 0 or not isinstance(block_id, str)
                or not re.fullmatch(r"[0-9a-f]{64}", block_id)
                or block_id[:16] != format(number, "016x")):
            raise ValueError("invalid block identity")
        return number, block_id, raw.get("parentHash")
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise MulticallError(502, "invalid Multicall node checkpoint") from error


def multicall_token_result(value):
    result = value.get("result")
    if not isinstance(result, dict) or type(result.get("result")) is not bool:
        raise MulticallError(502, "invalid Multicall token envelope")
    returned = value.get("constant_result")
    if (not isinstance(returned, list) or len(returned) != 1
            or not isinstance(returned[0], str) or len(returned[0]) % 2
            or not WORD_HEX.fullmatch(returned[0])):
        raise MulticallError(502, "invalid Multicall token envelope")
    if result["result"] is False:
        # Native VM timeout uses the same code but has no terminal transaction/
        # return-data proof. Never turn preflight, timeout or infrastructure errors
        # into a zero balance.
        transaction = value.get("transaction")
        ret = transaction.get("ret") if isinstance(transaction, dict) else None
        terminal = (isinstance(ret, list) and len(ret) == 1 and isinstance(ret[0], dict)
                    and ret[0].get("contractRet") in ("REVERT", "FAILED", "TRANSFER_FAILED"))
        energy = value.get("energy_used")
        if (result.get("code") == "CONTRACT_EXE_ERROR" and terminal
                and type(energy) is int and energy >= 0):
            return 0, True
        raise MulticallError(502, "Multicall token execution is unavailable")
    if len(returned[0]) != 64:
        # Actual successful calls to absent code or a non-TRC20 token return no
        # uint256 word. This is a per-token result, not an invented balance.
        return 0, True
    return int(returned[0], 16), False


def multicall_result(balances, errors):
    word = lambda value: format(value, "064x")
    count = len(balances)
    encoded = (word(64) + word(96 + 32 * count) + word(count)
               + "".join(word(value) for value in balances) + word(count)
               + "".join(word(int(value)) for value in errors))
    return {"result": {"result": True}, "constant_result": [encoded]}




def getblock_request(route, body):
    """Narrow read-only compatibility for TronLink's pre-signature TAPOS checks."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate getblock field")
            result[key] = value
        return result
    request = json.loads(body or b"{}", object_pairs_hook=pairs)
    if (not isinstance(request, dict) or set(request) - {"id_or_num", "detail"}
            or request.get("detail", False) is not False):
        raise ValueError("getblock supports only detail:false and a decimal height string")
    prefix = route.rsplit("/", 1)[0]
    if "id_or_num" not in request:
        return prefix + "/getnowblock", b"{}", None
    selector = request["id_or_num"]
    if (not isinstance(selector, str) or not re.fullmatch(r"0|[1-9][0-9]{0,18}", selector)
            or int(selector) > 9223372036854775807):
        raise ValueError("getblock height must be a canonical nonnegative decimal string")
    height = int(selector)
    return prefix + "/getblockbynum", json.dumps({"num": height}).encode(), height


def getblock_header(data, height):
    """Preserve the canonical native header/id; never substitute head for solid."""
    block = json.loads(data)
    if block == {}:
        return b"{}"  # Missing or above-solid block: retain native empty-result semantics.
    block_id = block.get("blockID") if isinstance(block, dict) else None
    header = block.get("block_header") if isinstance(block, dict) else None
    number = header.get("raw_data", {}).get("number") if isinstance(header, dict) else None
    if (not isinstance(block_id, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", block_id)
            or type(number) is not int or number < 0 or int(block_id[:16], 16) != number
            or (height is not None and number != height)):
        raise ValueError("invalid native block header")
    return json.dumps({"blockID": block_id, "block_header": header}, separators=(",", ":")).encode()

def checked_origin(value):
    if not isinstance(value, str) or not value or any(ord(c) < 33 for c in value):
        raise ValueError("origins must be nonempty exact origin strings")
    if value == CHROME_ORIGIN:
        return value
    parsed = urlsplit(value)
    if (parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment
            or not parsed.hostname or parsed.scheme not in ("http", "https")):
        raise ValueError("origin must contain only scheme, host, and optional port")
    _ = parsed.port  # Validate port syntax and range.
    if parsed.scheme == "http" and parsed.hostname != "localhost":
        try:
            if not ipaddress.ip_address(parsed.hostname).is_loopback:
                raise ValueError("HTTP origins must be explicit loopback development origins")
        except ValueError as error:
            raise ValueError("HTTP origins must be explicit loopback development origins") from error
    return value


@dataclass(frozen=True)
class Config:
    listen_host: str = "127.0.0.1"
    listen_port: int = 19180
    upstream_host: str = "127.0.0.1"
    upstream_port: int = 19120
    allowed_origins: tuple = ()
    max_body_bytes: int = 2 * 1024 * 1024
    max_response_bytes: int = 16 * 1024 * 1024
    request_timeout_seconds: float = 30
    upstream_timeout_seconds: float = 20
    max_connections: int = 16
    tronlink_multicall_enabled: bool = False
    tronlink_multicall_profile_file: str | None = None

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict) or set(data) - {f.name for f in fields(cls)}:
            raise ValueError("unknown or invalid gateway configuration fields")
        cfg = cls(**data)
        # IP literals avoid DNS remapping. This gateway is deliberately IPv4 loopback only.
        if cfg.listen_host != "127.0.0.1" or cfg.upstream_host != "127.0.0.1":
            raise ValueError("listen_host and upstream_host must be 127.0.0.1")
        for name in ("listen_port", "upstream_port"):
            n = getattr(cfg, name)
            if type(n) is not int or not 0 <= n <= 65535 or (name == "upstream_port" and n == 0):
                raise ValueError("invalid loopback port")
        for name, floor, cap in (
            ("max_body_bytes", 1, 8 * 1024 * 1024),
            ("max_response_bytes", 1, 64 * 1024 * 1024),
            ("max_connections", 1, 128),
        ):
            n = getattr(cfg, name)
            if type(n) is not int or not floor <= n <= cap:
                raise ValueError("invalid " + name)
        for name in ("request_timeout_seconds", "upstream_timeout_seconds"):
            n = getattr(cfg, name)
            if type(n) not in (int, float) or not 0 < n <= 120:
                raise ValueError("invalid " + name)
        if type(cfg.tronlink_multicall_enabled) is not bool:
            raise ValueError("tronlink_multicall_enabled must be a boolean")
        marker = cfg.tronlink_multicall_profile_file
        if marker is not None and (not isinstance(marker, str) or not Path(marker).is_absolute()):
            raise ValueError("Multicall private profile path must be absolute")
        if cfg.tronlink_multicall_enabled and marker is None:
            raise ValueError("enabled Multicall requires a private profile marker")
        if not isinstance(cfg.allowed_origins, (list, tuple)):
            raise ValueError("allowed_origins must be a list")
        for origin in cfg.allowed_origins:
            checked_origin(origin)
        return cfg

    @property
    def origins(self):
        return frozenset((CHROME_ORIGIN, *self.allowed_origins))


class Gateway(server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, config):
        # Revalidate dataclass instances too, including direct test/programmatic use.
        self.config = Config.from_dict({f.name: getattr(config, f.name) for f in fields(Config)})
        self.slots = threading.BoundedSemaphore(self.config.max_connections)
        super().__init__((self.config.listen_host, self.config.listen_port), Handler)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()

    def handle_error(self, request, address):
        # Do not log request payloads, authorization headers, or Python tracebacks.
        pass


class Handler(server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TronWalletGateway"
    sys_version = ""

    def handle(self):
        self.connection.settimeout(self.server.config.request_timeout_seconds)
        self.upstream = None
        self.upstream_socket = None
        self.request_deadline = time.monotonic() + self.server.config.request_timeout_seconds
        self.aborted = False
        timer = threading.Timer(self.server.config.request_timeout_seconds, self.abort)
        timer.daemon = True
        timer.start()
        try:
            super().handle()
        finally:
            timer.cancel()
            if self.upstream:
                self.upstream.close()

    def abort(self):
        # Absolute request deadline, including slowly arriving headers/body/response.
        self.aborted = True
        sockets = [self.connection]
        if self.upstream_socket:
            sockets.append(self.upstream_socket)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def log_message(self, *args):
        pass

    def reply(self, status, value=None, origin=None, preflight=False):
        data = b"" if value is None else json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Vary", "Origin")
        if origin in self.server.config.origins:
            self.send_header("Access-Control-Allow-Origin", origin)
        if preflight:
            self.send_header("Access-Control-Allow-Methods", ", ".join(sorted(ENDPOINTS[self.route])))
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()
        if data:
            self.wfile.write(data)
        self.close_connection = True

    def error(self, status, message, origin=None):
        self.reply(status, {"Error": message}, origin)

    def multicall_remaining(self, deadline):
        remaining = min(deadline, self.request_deadline) - time.monotonic()
        if self.aborted or remaining <= 0:
            raise MulticallError(504, "private Multicall request deadline exceeded")
        return remaining

    def multicall_native(self, route, payload, deadline):
        timeout = min(self.multicall_remaining(deadline), self.server.config.upstream_timeout_seconds)
        self.upstream = client.HTTPConnection(
            self.server.config.upstream_host, self.server.config.upstream_port, timeout=timeout)
        try:
            self.upstream.request("POST", route, body=json.dumps(payload).encode(),
                                  headers={"Content-Type": "application/json", "Accept": "application/json"})
            self.upstream_socket = self.upstream.sock
            response = self.upstream.getresponse()
            data = response.read(self.server.config.max_response_bytes + 1)
            self.multicall_remaining(deadline)
            if response.status != 200 or len(data) > self.server.config.max_response_bytes:
                raise MulticallError(502, "private Multicall node read failed")
            try:
                value = strict_json(data)
            except (ValueError, UnicodeError) as error:
                raise MulticallError(502, "invalid private Multicall node response") from error
            if not isinstance(value, dict) or "Error" in value or "error" in value:
                raise MulticallError(502, "private Multicall node read failed")
            return value
        except (TimeoutError, socket.timeout) as error:
            raise MulticallError(504, "private Multicall node read timed out") from error
        except (OSError, client.HTTPException) as error:
            raise MulticallError(502, "private Multicall node read failed") from error
        finally:
            self.upstream.close()
            self.upstream_socket = None

    def compose_multicall(self, request):
        cfg = self.server.config
        deadline = min(self.request_deadline, time.monotonic() + cfg.upstream_timeout_seconds)
        profile = private_multicall_profile(cfg.tronlink_multicall_profile_file)
        genesis = multicall_checkpoint(self.multicall_native("/wallet/getblockbynum", {"num": 0}, deadline))
        if genesis[0] != 0 or (genesis[1], genesis[2]) != profile:
            raise MulticallError(503, "private Multicall node/profile identity differs")
        owner, wallet, tokens = request
        for _attempt in range(2):
            before = multicall_checkpoint(self.multicall_native("/wallet/getnowblock", {}, deadline))
            balances, errors = [], []
            for token in tokens:
                value = self.multicall_native("/wallet/triggerconstantcontract", {
                    "contract_address": token, "owner_address": owner,
                    "function_selector": "balanceOf(address)", "parameter": wallet, "call_value": 0,
                }, deadline)
                balance, error = multicall_token_result(value)
                balances.append(balance)
                errors.append(error)
            after = multicall_checkpoint(self.multicall_native("/wallet/getnowblock", {}, deadline))
            if before == after:
                self.multicall_remaining(deadline)
                if private_multicall_profile(cfg.tronlink_multicall_profile_file) != profile:
                    raise MulticallError(503, "private Multicall profile changed during request")
                return multicall_result(balances, errors)
        raise MulticallError(503, "private Multicall canonical head changed during request")

    def dispatch(self):
        if len(self.path) > 8192:
            return self.error(414, "request URI is too long")
        parsed = urlsplit(self.path)
        # Reject absolute-form proxy targets, escaped paths, and path suffix tricks.
        if parsed.scheme or parsed.netloc or parsed.fragment or "%" in parsed.path:
            return self.error(404, "wallet endpoint is not allowed")
        self.route = parsed.path
        allowed = ENDPOINTS.get(self.route)
        origin_headers = self.headers.get_all("Origin", [])
        origin = origin_headers[0] if len(origin_headers) == 1 else None
        if len(origin_headers) > 1 or (origin is not None and origin not in self.server.config.origins):
            return self.error(403, "origin is not allowed")
        if allowed is None:
            return self.error(404, "wallet endpoint is not allowed", origin)
        if self.command == "OPTIONS":
            method = self.headers.get("Access-Control-Request-Method", "")
            headers = {h.strip().lower() for h in self.headers.get("Access-Control-Request-Headers", "").split(",") if h.strip()}
            if method not in allowed or headers - {"content-type"}:
                return self.error(403, "preflight method or headers are not allowed", origin)
            return self.reply(204, origin=origin, preflight=True)
        if self.command not in allowed:
            return self.error(405, "wallet method is not allowed", origin)
        lengths = self.headers.get_all("Content-Length", [])
        if self.headers.get("Transfer-Encoding") or len(lengths) > 1:
            return self.error(400, "ambiguous request framing", origin)
        if self.command == "POST" and not lengths:
            return self.error(411, "Content-Length is required", origin)
        try:
            size = int(lengths[0]) if lengths else 0
        except ValueError:
            return self.error(400, "invalid Content-Length", origin)
        if size < 0:
            return self.error(400, "invalid Content-Length", origin)
        if size > self.server.config.max_body_bytes:
            return self.error(413, "wallet request body is too large", origin)
        if self.command == "GET" and size:
            return self.error(400, "GET body is not allowed", origin)
        try:
            body = self.rfile.read(size) if size else None
            if body is not None and len(body) != size:
                return self.error(400, "incomplete wallet request body", origin)
            if (self.server.config.tronlink_multicall_enabled and self.command == "POST"
                    and self.route == "/wallet/triggerconstantcontract"):
                try:
                    composed = multicall_request(body)
                    if composed is not None:
                        if parsed.query:
                            raise MulticallError(400, "private Multicall query parameters are not allowed")
                        return self.reply(200, self.compose_multicall(composed), origin=origin)
                except MulticallError as error:
                    return self.error(error.status, str(error), origin)
            request_path = self.path
            projected = self.route in GETBLOCK_ROUTES
            height = None
            if projected:
                if parsed.query:
                    return self.error(400, "getblock query parameters are not allowed", origin)
                try:
                    request_path, body, height = getblock_request(self.route, body)
                except (ValueError, UnicodeError, TypeError):
                    return self.error(400, "invalid getblock selector or schema", origin)
            self.upstream = client.HTTPConnection(
                self.server.config.upstream_host, self.server.config.upstream_port,
                timeout=self.server.config.upstream_timeout_seconds)
            # No caller cookies/authorization headers, redirect handling, or automatic retries.
            self.upstream.request(self.command, request_path, body=body,
                                  headers={"Content-Type": "application/json", "Accept": "application/json"})
            self.upstream_socket = self.upstream.sock
            response = self.upstream.getresponse()
            data = response.read(self.server.config.max_response_bytes + 1)
            if len(data) > self.server.config.max_response_bytes:
                return self.error(502, "wallet upstream response is too large", origin)
            if projected and response.status == 200:
                try:
                    data = getblock_header(data, height)
                except (ValueError, TypeError, AttributeError):
                    return self.error(502, "invalid native getblock response", origin)
            self.send_response(response.status)
            self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Vary", "Origin")
            if origin in self.server.config.origins:
                self.send_header("Access-Control-Allow-Origin", origin)
            self.end_headers()
            self.wfile.write(data)
            self.close_connection = True
        except (TimeoutError, socket.timeout):
            self.error(504, "wallet upstream timeout; broadcast outcome may be unknown", origin)
        except (OSError, client.HTTPException):
            self.error(502, "wallet upstream unavailable; broadcast outcome may be unknown", origin)
        finally:
            if self.upstream:
                self.upstream.close()

    do_GET = dispatch
    do_POST = dispatch
    do_OPTIONS = dispatch
    do_PUT = dispatch
    do_DELETE = dispatch
    do_PATCH = dispatch
    do_HEAD = dispatch
    do_CONNECT = dispatch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    config = Config.from_dict(json.loads(args.config.read_text()))
    with Gateway(config) as gateway:
        print(f"wallet gateway listening on 127.0.0.1:{gateway.server_port}; upstream 127.0.0.1:{config.upstream_port}", flush=True)
        try:
            gateway.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        report_fatal("wallet_gateway_failed", "Wallet gateway terminated with an error", error)
        raise
