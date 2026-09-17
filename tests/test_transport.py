"""Scene-free transport regressions using only local socket pairs."""
import json
import socket
import threading
import time
import unittest
from unittest.mock import Mock, patch
from sketchup_mcp.server import (
    SketchupConnection, SketchupTransportError, SketchupTimeoutError,
    SketchupRubyError,
)


class TransportTests(unittest.TestCase):
    def setUp(self):
        client, self.peer = socket.socketpair()
        self.connection = SketchupConnection(sock=client)
        self.addCleanup(self.connection.disconnect)
        self.addCleanup(self.peer.close)

    def serve(self, action):
        def run():
            try:
                action(self.peer)
            except (BrokenPipeError, OSError):
                pass
        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        self.addCleanup(lambda: worker.join(1))

    @staticmethod
    def request(peer):
        wire = b""
        while not wire.endswith(b"\n"):
            wire += peer.recv(4096)
        return json.loads(wire)

    def test_wrong_id_is_read_past_without_resending(self):
        def action(peer):
            req = self.request(peer)
            peer.sendall(json.dumps({"id": 999, "result": {}}).encode() +
                         json.dumps({"id": req["id"], "result": {"created": 1}}).encode())
        self.serve(action)
        self.assertEqual(self.connection.send_command("create_component"), {"created": 1})
        self.peer.settimeout(0.05)
        with self.assertRaises(socket.timeout):
            self.peer.recv(4096)  # No duplicate command queued.

    def test_lost_response_is_not_replayed(self):
        def action(peer):
            self.request(peer)
            peer.shutdown(socket.SHUT_RDWR)
        self.serve(action)
        with patch.object(self.connection, "connect") as reconnect:
            with self.assertRaisesRegex(SketchupTransportError, "not replayed"):
                self.connection.send_command("create_component")
            reconnect.assert_not_called()
        self.assertIsNone(self.connection.sock)

    def test_slow_partial_response_obeys_total_deadline(self):
        def action(peer):
            self.request(peer)
            for byte in b'{"id":1,"result":{}}':
                peer.sendall(bytes([byte]))
                time.sleep(0.03)
        self.serve(action)
        started = time.monotonic()
        with self.assertRaisesRegex(SketchupTimeoutError, "not replayed"):
            self.connection.send_command("create_component", timeout=0.12)
        self.assertLess(time.monotonic() - started, 0.4)

    def test_ruby_error_then_connection_reuse(self):
        def action(peer):
            req = self.request(peer)
            peer.sendall(json.dumps({"id": req["id"], "error": {"code": -32002, "message": "fixture", "data": {"backtrace": ["fixture:1"]}}}).encode() + b"\n")
            req = self.request(peer)
            peer.sendall(json.dumps({"id": req["id"], "result": {"pong": True}}).encode() + b"\n")
        self.serve(action)
        with self.assertRaises(SketchupRubyError) as result:
            self.connection.send_command("eval_ruby")
        self.assertEqual(result.exception.backtrace, ["fixture:1"])
        self.assertEqual(self.connection.send_command("ping"), {"pong": True})

    def test_send_failure_is_typed_and_socket_closed(self):
        self.connection.disconnect()
        sock = Mock()
        sock.sendall.side_effect = BrokenPipeError("fixture")
        self.connection.sock = sock
        with self.assertRaisesRegex(SketchupTransportError, "not replayed"):
            self.connection.send_command("create_component")
        sock.close.assert_called_once()

    def test_multibyte_and_escaped_braces_framing(self):
        value = {"id": 1, "result": {"name": 'مكتب } "desk"'}}
        data = json.dumps(value, ensure_ascii=False).encode()
        for split in range(len(data)):
            self.assertEqual(SketchupConnection._try_parse_prefix(data[:split]), (None, 0))
        self.assertEqual(SketchupConnection._try_parse_prefix(data), (value, len(data)))


if __name__ == "__main__":
    unittest.main()
