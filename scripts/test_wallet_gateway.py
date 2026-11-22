"""Real loopback HTTP tests; the upstream is a fake server, never a TRON node."""
from dataclasses import replace
from http import client, server
import json
import socket
import threading
import time
import unittest

from wallet_gateway import CHROME_ORIGIN, Config, Gateway


class Upstream(server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.respond()

    def do_GET(self):
        self.respond()

    def respond(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        body = json.loads(raw) if raw else None
        self.server.calls.append((self.command, self.path, raw, dict(self.headers)))
        if body and body.get("slow"):
            time.sleep(0.3)
        if body and body.get("trickle"):
            try:
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                for _ in range(20):
                    self.wfile.write(b"12345")
                    self.wfile.flush()
                    time.sleep(0.05)
            except OSError:
                pass
            return
        if getattr(self.server, "block_compat", False) and self.path.endswith(("/getnowblock", "/getblockbynum")):
            solid = self.path.startswith("/walletsolidity/")
            limit = 80 if solid else 100
            height = body["num"] if self.path.endswith("/getblockbynum") else limit
            block = {} if height > limit else {
                "blockID": f"{height:016x}" + "b" * 48,
                "block_header": {"raw_data": {"number": height, "timestamp": 1700000000000}},
                "transactions": [{"txID": "c" * 64}],
            }
            if block and getattr(self.server, "bad_block_header", False):
                block["block_header"]["raw_data"]["number"] += 1
            data = json.dumps(block).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        data = b"x" * 2048 if body and body.get("large") else json.dumps({
            "path": self.path, "body": body,
            "cookie": self.headers.get("Cookie"),
            "authorization": self.headers.get("Authorization"),
            "api_key": self.headers.get("TRON-PRO-API-KEY"),
            "content_type": self.headers.get("Content-Type"),
        }).encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            # The gateway must not forward either upstream policy/cookie header.
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Set-Cookie", "private=value")
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            pass


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.upstream = server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        self.upstream.daemon_threads = True
        self.upstream.calls = []
        self.ut = threading.Thread(target=self.upstream.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.ut.start()
        self.config = Config(
            listen_port=0, upstream_port=self.upstream.server_port,
            allowed_origins=("https://app.example", "http://localhost:3000"),
            max_body_bytes=128, max_response_bytes=1024,
            upstream_timeout_seconds=0.1, request_timeout_seconds=1)
        self.gateway = Gateway(self.config)
        self.gt = threading.Thread(target=self.gateway.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.gt.start()

    def tearDown(self):
        self.gateway.shutdown()
        self.gateway.server_close()
        self.upstream.shutdown()
        self.upstream.server_close()
        self.gt.join(timeout=2)
        self.ut.join(timeout=2)

    def request(self, method="GET", path="/wallet/getnowblock", body=None, headers=None):
        conn = client.HTTPConnection("127.0.0.1", self.gateway.server_port, timeout=3)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            raw = response.read()
            return response.status, dict(response.getheaders()), json.loads(raw) if raw else None
        finally:
            conn.close()

    def test_default_loopback_addresses_and_private_configuration_validation(self):
        self.assertEqual(Config().listen_host, "127.0.0.1")
        self.assertEqual(Config().listen_port, 19180)
        self.assertEqual(Config().upstream_port, 19120)
        for data in [
            {"listen_host": "0.0.0.0"}, {"upstream_host": "example.com"},
            {"allowed_origins": ["null"]}, {"allowed_origins": ["*"]},
            {"allowed_origins": ["https://app.example/"]},
            {"allowed_origins": ["http://app.example"]},
            {"allowed_origins": ["https://user:secret@app.example"]},
            {"allowed_origins": ["https://app.example:99999"]},
            {"allowed_origins": "https://app.example"},
            {"request_timeout_seconds": 0}, {"request_timeout_seconds": float("nan")},
            {"upstream_port": 0}, {"unknown": True},
        ]:
            with self.subTest(data=data), self.assertRaises(ValueError):
                Config.from_dict(data)
        with self.assertRaises(ValueError):
            Gateway(replace(self.config, listen_host="0.0.0.0"))

    def test_originless_requests_work_without_wildcard_or_credentials(self):
        status, headers, body = self.request()
        self.assertEqual(status, 200)
        self.assertEqual(body["path"], "/wallet/getnowblock")
        self.assertNotIn("Access-Control-Allow-Origin", headers)
        self.assertNotIn("Access-Control-Allow-Credentials", headers)
        self.assertNotIn("Set-Cookie", headers)

    def test_exact_official_extension_and_configured_site_origins(self):
        for origin in [CHROME_ORIGIN, "https://app.example", "http://localhost:3000"]:
            with self.subTest(origin=origin):
                status, headers, _ = self.request(headers={"Origin": origin})
                self.assertEqual(status, 200)
                self.assertEqual(headers["Access-Control-Allow-Origin"], origin)
                self.assertEqual(headers["Vary"], "Origin")
                self.assertNotIn("Access-Control-Allow-Credentials", headers)

    def test_untrusted_null_and_lookalike_origins_do_not_reach_upstream(self):
        for origin in ["null", "https://evil.example", "https://app.example.evil",
                       "chrome-extension://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]:
            status, headers, _ = self.request(headers={"Origin": origin})
            self.assertEqual(status, 403)
            self.assertNotIn("Access-Control-Allow-Origin", headers)
        self.assertEqual(self.upstream.calls, [])

    def test_options_preflight_is_local_and_precise(self):
        status, headers, _ = self.request("OPTIONS", "/wallet/broadcasttransaction", headers={
            "Origin": CHROME_ORIGIN, "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type"})
        self.assertEqual(status, 204)
        self.assertEqual(headers["Access-Control-Allow-Methods"], "POST")
        self.assertEqual(headers["Access-Control-Allow-Headers"], "Content-Type")
        self.assertNotIn("Access-Control-Allow-Credentials", headers)
        self.assertEqual(self.upstream.calls, [])

    def test_preflight_rejects_unapproved_method_or_auth_headers(self):
        for method, headers in [("DELETE", "content-type"), ("POST", "Authorization"),
                                ("POST", "TRON-PRO-API-KEY")]:
            status, _, _ = self.request("OPTIONS", "/wallet/broadcasttransaction", headers={
                "Origin": CHROME_ORIGIN, "Access-Control-Request-Method": method,
                "Access-Control-Request-Headers": headers})
            self.assertEqual(status, 403)
        self.assertEqual(self.upstream.calls, [])

    def test_admin_debug_raw_rpc_key_generation_and_path_tricks_are_blocked(self):
        for path in ["/", "/admin/stop", "/debug", "/v1/archive/account", "/wallet/getspendingkey",
                     "/wallet/getnowblock/", "/wallet/%67etnowblock", "//example.com/wallet/getnowblock",
                     "http://example.com/wallet/getnowblock"]:
            status, _, _ = self.request(path=path)
            self.assertEqual(status, 404, path)
        self.assertEqual(self.upstream.calls, [])

    def test_methods_and_request_body_are_bounded_before_proxying(self):
        status, _, _ = self.request("GET", "/wallet/getaccount")
        self.assertEqual(status, 405)
        status, _, _ = self.request("DELETE", "/wallet/broadcasttransaction")
        self.assertEqual(status, 405)
        status, _, _ = self.request("POST", "/wallet/broadcasttransaction", b"x" * 129)
        self.assertEqual(status, 413)
        self.assertEqual(self.upstream.calls, [])

    def test_signweight_is_exact_wallet_post_read_and_preserves_unsigned_json(self):
        raw = b'{"visible":false,"raw_data":{"contract":[]},"raw_data_hex":"fixture","txID":"fixture"}'
        status, _, body = self.request("POST", "/wallet/getsignweight", raw,
                                       {"Origin": CHROME_ORIGIN})
        self.assertEqual(status, 200)
        self.assertEqual(body["path"], "/wallet/getsignweight")
        self.assertEqual(self.upstream.calls[-1][2], raw)
        count = len(self.upstream.calls)
        self.assertEqual(self.request("GET", "/wallet/getsignweight")[0], 405)
        self.assertEqual(self.request("POST", "/walletsolidity/getsignweight", raw)[0], 404)
        self.assertEqual(len(self.upstream.calls), count)

    def test_signed_broadcast_is_forwarded_once_without_credentials(self):
        raw = b'{"txID":"test-fixture","signature":["fixture"],"raw_data":{}}'
        status, _, body = self.request("POST", "/wallet/broadcasttransaction", raw, {
            "Origin": CHROME_ORIGIN, "Cookie": "secret=value", "Authorization": "private",
            "TRON-PRO-API-KEY": "private",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["body"]["signature"], ["fixture"])
        self.assertIsNone(body["cookie"])
        self.assertIsNone(body["authorization"])
        self.assertIsNone(body["api_key"])
        self.assertEqual(body["content_type"], "application/json")
        self.assertEqual(len(self.upstream.calls), 1)
        self.assertEqual(self.upstream.calls[0][2], raw)

    def test_confirmed_wallet_queries_preserve_query_and_post_json(self):
        status, _, body = self.request("POST", "/walletsolidity/getaccount?visible=true",
                                       b'{"address":"fixture","visible":true}')
        self.assertEqual(status, 200)
        self.assertEqual(body["path"], "/walletsolidity/getaccount?visible=true")
        self.assertEqual(body["body"]["address"], "fixture")

    def test_tronlink_getblock_tapos_reads_project_exact_canonical_headers(self):
        self.upstream.block_compat = True
        examples = [
            ("/wallet/getblock", {"detail": False}, "/wallet/getnowblock", {}, 100),
            ("/walletsolidity/getblock", {"detail": False}, "/walletsolidity/getnowblock", {}, 80),
            ("/wallet/getblock", {"id_or_num": "82", "detail": False},
             "/wallet/getblockbynum", {"num": 82}, 82),
            ("/wallet/getblock", {"id_or_num": "0", "detail": False},
             "/wallet/getblockbynum", {"num": 0}, 0),
        ]
        for route, body, target, upstream_body, height in examples:
            with self.subTest(route=route, body=body):
                status, headers, result = self.request("POST", route, json.dumps(body).encode(),
                                                       {"Origin": CHROME_ORIGIN})
                self.assertEqual(status, 200)
                self.assertEqual(headers["Access-Control-Allow-Origin"], CHROME_ORIGIN)
                self.assertEqual(result, {
                    "blockID": f"{height:016x}" + "b" * 48,
                    "block_header": {"raw_data": {"number": height, "timestamp": 1700000000000}},
                })
                self.assertEqual(self.upstream.calls[-1][:3],
                                 ("POST", target, json.dumps(upstream_body).encode()))

    def test_getblock_strict_schema_selectors_fail_before_any_upstream_request(self):
        bodies = [
            b'[]', b'null', b'{"detail":false,"detail":false}', b'{"detail":true}',
            b'{"detail":0}', b'{"detail":null}', b'{"detail":false,"owner":"unused"}',
            b'{"id_or_num":-1}', b'{"id_or_num":1}', b'{"id_or_num":1.5}',
            b'{"id_or_num":null}', b'{"id_or_num":""}', b'{"id_or_num":"-1"}',
            b'{"id_or_num":"01"}', b'{"id_or_num":"1e3"}', b'{"id_or_num":"0x1"}',
            b'{"id_or_num":"9223372036854775808"}', b'{"id_or_num":"1","visible":true}',
        ]
        for raw in bodies:
            with self.subTest(body=raw):
                self.assertEqual(self.request("POST", "/wallet/getblock", raw)[0], 400)
        self.assertEqual(self.request("POST", "/wallet/getblock?detail=false", b'{}')[0], 400)
        self.assertEqual(self.request("GET", "/wallet/getblock")[0], 405)
        self.assertEqual(self.request("PUT", "/wallet/getblock", b'{}')[0], 405)
        self.assertEqual(self.upstream.calls, [])

    def test_getblock_solidity_never_falls_back_to_head_or_returns_unconfirmed_height(self):
        self.upstream.block_compat = True
        status, _, result = self.request("POST", "/walletsolidity/getblock",
                                        b'{"id_or_num":"81","detail":false}')
        self.assertEqual(status, 200)
        self.assertEqual(result, {})
        self.assertEqual([x[1] for x in self.upstream.calls], ["/walletsolidity/getblockbynum"])

    def test_getblock_rejects_mismatched_native_header_without_substitution(self):
        self.upstream.block_compat = True
        self.upstream.bad_block_header = True
        status, _, result = self.request("POST", "/wallet/getblock", b'{"detail":false}')
        self.assertEqual(status, 502)
        self.assertIn("invalid native", result["Error"])
        self.assertEqual(len(self.upstream.calls), 1)

    def test_oversized_upstream_response_fails_without_streaming_it(self):
        status, _, body = self.request("POST", "/wallet/getblockbynum", b'{"large":true}')
        self.assertEqual(status, 502)
        self.assertIn("too large", body["Error"])

    def test_timeout_does_not_retry_an_unknown_broadcast_outcome(self):
        status, _, body = self.request("POST", "/wallet/broadcasttransaction", b'{"slow":true}')
        self.assertEqual(status, 504)
        self.assertIn("outcome may be unknown", body["Error"])
        self.assertEqual(len(self.upstream.calls), 1)

    def test_absolute_deadline_also_stops_trickling_upstream_response(self):
        self.gateway.config = replace(self.config, request_timeout_seconds=0.15,
                                      upstream_timeout_seconds=1)
        start = time.monotonic()
        try:
            self.request("POST", "/wallet/getblockbynum", b'{"trickle":true}')
        except (client.HTTPException, OSError):
            pass  # Deadline closes the client connection before any buffered response.
        self.assertLess(time.monotonic() - start, 0.8)
        self.assertEqual(len(self.upstream.calls), 1)

    def test_ambiguous_transfer_framing_is_rejected(self):
        conn = client.HTTPConnection("127.0.0.1", self.gateway.server_port, timeout=2)
        try:
            conn.putrequest("POST", "/wallet/getaccount")
            conn.putheader("Content-Length", "2")
            conn.putheader("Content-Length", "2")
            conn.endheaders(b"{}")
            response = conn.getresponse()
            self.assertEqual(response.status, 400)
            response.read()
        finally:
            conn.close()
        self.assertEqual(self.upstream.calls, [])

    def test_absolute_deadline_closes_an_incomplete_request_body(self):
        conn = socket.create_connection(("127.0.0.1", self.gateway.server_port), timeout=3)
        start = time.monotonic()
        try:
            conn.sendall(b"POST /wallet/getaccount HTTP/1.1\r\nHost: localhost\r\nContent-Length: 20\r\n\r\n{")
            try:
                while conn.recv(4096):
                    pass
            except ConnectionResetError:
                pass
            self.assertLess(time.monotonic() - start, 2.5)
        finally:
            conn.close()
        self.assertEqual(self.upstream.calls, [])


if __name__ == "__main__":
    unittest.main()
