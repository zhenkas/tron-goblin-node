"""Real loopback HTTP composition tests; never a live node or wallet."""
from dataclasses import replace
from http import client, server
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from wallet_gateway import CHROME_ORIGIN, Config, Gateway, MULTICALL_ADDRESS, MULTICALL_SIGNATURE

GENESIS = "0" * 16 + "a" * 48
PROFILE = "b" * 64
OWNER = "41" + "1" * 40
WALLET = "0" * 24 + "2" * 40
TOKENS = ["41" + digit * 40 for digit in "3456"]


def word(n):
    return format(n, "064x")


def request_body(tokens=None):
    tokens = TOKENS[:1] if tokens is None else tokens
    return {"contract_address": MULTICALL_ADDRESS, "owner_address": OWNER,
            "function_selector": MULTICALL_SIGNATURE, "call_value": 0,
            "parameter": word(64) + WALLET + word(len(tokens))
            + "".join("0" * 24 + token[2:] for token in tokens)}


def success(raw):
    return {"result": {"result": True}, "constant_result": [raw],
            "energy_used": 10, "transaction": {"ret": [{"contractRet": "SUCCESS"}]}}


def decode(body):
    raw = body["constant_result"][0]
    words = [int(raw[i:i + 64], 16) for i in range(0, len(raw), 64)]
    a, b = words[0] // 32, words[1] // 32
    return words[a + 1:a + 1 + words[a]], words[b + 1:b + 1 + words[b]]


class Native(server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        body = json.loads(raw)
        self.server.calls.append((self.path, body, dict(self.headers)))
        status, delay = 200, 0
        if self.path == "/wallet/getblockbynum":
            value = {"blockID": self.server.genesis, "block_header": {
                "raw_data": {"number": 0, "parentHash": self.server.profile}}}
        elif self.path == "/wallet/getnowblock":
            height = self.server.heads.pop(0) if self.server.heads else 10
            value = {"blockID": format(height, "016x") + "c" * 48,
                     "block_header": {"raw_data": {"number": height, "parentHash": "d" * 64}}}
        elif self.path == "/wallet/triggerconstantcontract" and body.get("function_selector") == "balanceOf(address)":
            if self.server.on_token:
                self.server.on_token()
            status, value, delay = self.server.tokens.get(body["contract_address"], (200, success(""), 0))
        else:
            value = {"ordinary": True, "body": body}
        if delay:
            time.sleep(delay)
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            pass


class MulticallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.marker = Path(self.temp.name) / "private-profile.json"
        self.write_marker()
        self.native = server.ThreadingHTTPServer(("127.0.0.1", 0), Native)
        self.native.daemon_threads = True
        self.native.calls, self.native.tokens, self.native.heads = [], {}, []
        self.native.genesis, self.native.profile = GENESIS, PROFILE
        self.native.on_token = None
        self.nt = threading.Thread(target=self.native.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.nt.start()
        self.config = Config(listen_port=0, upstream_port=self.native.server_port,
                             request_timeout_seconds=2, upstream_timeout_seconds=1,
                             tronlink_multicall_enabled=True,
                             tronlink_multicall_profile_file=str(self.marker))
        self.start_gateway()

    def write_marker(self, genesis=GENESIS, profile=PROFILE):
        self.marker.write_text(json.dumps({"schema": 1, "genesis": genesis,
                                          "profile_hash": profile, "native_unfreeze_delay_secs": 300}))
        self.marker.chmod(0o600)

    def start_gateway(self):
        self.gateway = Gateway(self.config)
        self.gt = threading.Thread(target=self.gateway.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.gt.start()

    def restart_gateway(self, **config):
        self.gateway.shutdown()
        self.gateway.server_close()
        self.gt.join(timeout=2)
        self.config = replace(self.config, **config)
        self.start_gateway()

    def tearDown(self):
        self.gateway.shutdown()
        self.gateway.server_close()
        self.native.shutdown()
        self.native.server_close()
        self.gt.join(timeout=2)
        self.nt.join(timeout=2)
        self.temp.cleanup()

    def call(self, body=None, path="/wallet/triggerconstantcontract", origin=CHROME_ORIGIN):
        data = body if isinstance(body, bytes) else json.dumps(request_body() if body is None else body).encode()
        conn = client.HTTPConnection("127.0.0.1", self.gateway.server_port, timeout=3)
        try:
            conn.request("POST", path, data, {"Origin": origin, "Content-Type": "application/json",
                                            "Cookie": "fixture", "Authorization": "fixture"})
            response = conn.getresponse()
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
        finally:
            conn.close()

    def test_stock_shape_uint256_zero_order_duplicates_and_header_stripping(self):
        self.native.tokens[TOKENS[0]] = (200, success(word((1 << 256) - 1)), 0)
        self.native.tokens[TOKENS[1]] = (200, success(word(0)), 0)
        status, result = self.call(request_body([TOKENS[0], TOKENS[1], TOKENS[0]]))
        self.assertEqual(status, 200)
        self.assertEqual(decode(result), ([(1 << 256) - 1, 0, (1 << 256) - 1], [0, 0, 0]))
        self.assertEqual(set(result), {"result", "constant_result"})
        calls = [entry for entry in self.native.calls if entry[0].endswith("triggerconstantcontract")]
        self.assertEqual([x[1]["contract_address"] for x in calls], [TOKENS[0], TOKENS[1], TOKENS[0]])
        for _, payload, headers in calls:
            self.assertEqual(payload, {"contract_address": payload["contract_address"], "owner_address": OWNER,
                                      "function_selector": "balanceOf(address)", "parameter": WALLET, "call_value": 0})
            self.assertNotIn("Cookie", headers)
            self.assertNotIn("Authorization", headers)

    def test_missing_reverting_malformed_token_and_real_balance_are_distinct(self):
        revert = {"result": {"result": False, "code": "CONTRACT_EXE_ERROR"}, "constant_result": [""],
                  "transaction": {"ret": [{"contractRet": "REVERT"}]}, "energy_used": 4}
        values = [success(""), revert, success("ff"), success(word(200000 * 10**18))]
        for token, value in zip(TOKENS, values):
            self.native.tokens[token] = (200, value, 0)
        status, body = self.call(request_body(TOKENS))
        self.assertEqual(status, 200)
        self.assertEqual(decode(body), ([0, 0, 0, 200000 * 10**18], [1, 1, 1, 0]))

    def test_infrastructure_and_malformed_envelopes_never_become_zero(self):
        invalid = [
            (503, {"Error": "unavailable"}), (200, {"Error": "unavailable"}),
            (200, {"result": {"result": False, "code": "CONTRACT_EXE_ERROR"}}),
            (200, {"result": {"result": False, "code": "CONTRACT_VALIDATE_ERROR"}, "constant_result": [""]}),
            (200, {"result": {"result": True}}), (200, success("zz")),
            (200, b'{"result":{"result":true,"result":true},"constant_result":[""]}'),
            (200, b"not JSON"),
        ]
        for status, value in invalid:
            with self.subTest(value=value):
                self.native.tokens[TOKENS[0]] = (status, value, 0)
                code, body = self.call()
                self.assertEqual(code, 502)
                self.assertNotIn("constant_result", body)

    def test_marker_is_reloaded_and_genesis_and_profile_both_verified(self):
        self.assertEqual(self.call()[0], 200)
        new_genesis, new_profile = "0" * 16 + "e" * 48, "f" * 64
        self.write_marker(new_genesis, new_profile)
        self.assertEqual(self.call()[0], 503)
        self.native.genesis, self.native.profile = new_genesis, new_profile
        self.assertEqual(self.call()[0], 200)
        self.native.profile = PROFILE
        self.assertEqual(self.call()[0], 503)

    def test_marker_missing_symlink_public_and_unsafe_fail_closed(self):
        self.marker.unlink()
        self.assertEqual(self.call()[0], 503)
        other = Path(self.temp.name) / "other"
        other.write_text("{}")
        self.marker.symlink_to(other)
        self.assertEqual(self.call()[0], 503)
        self.marker.unlink()
        self.write_marker("0" * 56 + "2b6653dc")
        self.assertEqual(self.call()[0], 503)
        self.write_marker()
        self.marker.chmod(0o666)
        self.assertEqual(self.call()[0], 503)
        self.assertEqual(self.native.calls, [])

    def test_marker_change_during_request_rejected(self):
        self.native.on_token = lambda: self.write_marker("0" * 16 + "e" * 48)
        self.assertEqual(self.call()[0], 503)

    def test_head_changes_retry_once_with_same_deadline_then_fail(self):
        self.native.heads = [10, 11, 11, 11]
        self.assertEqual(self.call()[0], 200)
        self.assertEqual(sum(x[0].endswith("triggerconstantcontract") for x in self.native.calls), 2)
        self.native.calls.clear()
        self.native.heads = [10, 11, 12, 13]
        self.assertEqual(self.call()[0], 503)
        self.assertEqual(sum(x[0].endswith("triggerconstantcontract") for x in self.native.calls), 2)

    def test_exact_candidate_rejects_schema_ambiguity_nonzero_value_and_abi_errors(self):
        bodies = []
        for value in (1, True, "0", 0.0):
            bodies.append(dict(request_body(), call_value=value))
        for key in ("visible", "data", "fee_limit", "call_token_value"):
            bodies.append(dict(request_body(), **{key: 0}))
        bodies.extend([
            dict(request_body(), owner_address="bad"),
            dict(request_body(), parameter=word(32) + WALLET + word(0)),
            dict(request_body(), parameter=word(64) + "1" + WALLET[1:] + word(0)),
            dict(request_body(), parameter=request_body()["parameter"] + "00"),
            request_body([TOKENS[0]] * 31),
            json.dumps(request_body()).replace('"call_value": 0', '"call_value": 0, "call_value": 0').encode(),
        ])
        for body in bodies:
            with self.subTest(body=body):
                self.assertEqual(self.call(body)[0], 400)
        self.assertEqual(self.call(path="/wallet/triggerconstantcontract?visible=true")[0], 400)
        self.assertEqual(self.native.calls, [])

    def test_empty_and_maximum_batches_decode_canonically(self):
        for tokens in ([], [TOKENS[0]] * 30):
            status, body = self.call(request_body(tokens))
            self.assertEqual(status, 200)
            self.assertEqual(decode(body), ([0] * len(tokens), [1] * len(tokens)))

    def test_overall_deadline_stops_subsequent_reads(self):
        self.restart_gateway(upstream_timeout_seconds=0.065, request_timeout_seconds=1)
        self.native.tokens[TOKENS[0]] = (200, success(word(1)), 0.04)
        started = time.monotonic()
        status, _ = self.call(request_body([TOKENS[0]] * 5))
        self.assertEqual(status, 504)
        self.assertLess(time.monotonic() - started, 0.3)
        count = len(self.native.calls)
        time.sleep(0.1)
        self.assertEqual(len(self.native.calls), count)
        self.assertLessEqual(sum(x[0].endswith("triggerconstantcontract") for x in self.native.calls), 2)

    def test_disabled_wrong_target_wrong_selector_other_namespace_and_broadcast_passthrough(self):
        bodies = [dict(request_body(), contract_address=TOKENS[0]),
                  dict(request_body(), function_selector="other()")]
        for body in bodies:
            code, result = self.call(body)
            self.assertEqual(code, 200)
            self.assertTrue(result["ordinary"])
        code, result = self.call({"unsigned_fixture": True}, path="/wallet/broadcasttransaction")
        self.assertEqual(code, 200)
        self.assertTrue(result["ordinary"])
        self.assertEqual(self.call(path="/walletsolidity/triggerconstantcontract")[0], 404)
        self.restart_gateway(tronlink_multicall_enabled=False)
        self.assertTrue(self.call()[1]["ordinary"])

    def test_origin_and_opt_in_configuration_are_strict(self):
        self.assertEqual(self.call(origin="https://foreign.example")[0], 403)
        self.assertEqual(self.native.calls, [])
        for data in ({"tronlink_multicall_enabled": True},
                     {"tronlink_multicall_enabled": "true"},
                     {"tronlink_multicall_profile_file": "relative.json"}):
            with self.subTest(data=data), self.assertRaises(ValueError):
                Config.from_dict(data)


if __name__ == "__main__":
    unittest.main()
