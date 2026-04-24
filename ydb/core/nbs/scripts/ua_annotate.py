#!/usr/bin/env python3
"""Minimal Unified Agent gRPC annotation client.

Sends a single log record to the Unified Agent `grpc` input via the native
``NUnifiedAgentProto.UnifiedAgentService/Session`` RPC
(see ``library/cpp/unified_agent_client/proto/unified_agent.proto``).

The script is intentionally tiny and has only one third-party dependency
(``grpcio``). If grpcio is missing we try once to ``pip install --user``
it -- on a machine running ydbd a working python3 + pip is practically
always present.

Usage:
    ua_annotate.py --uri HOST:PORT --message TEXT
                   [--meta KEY=VALUE ...]
                   [--timeout SEC]

Exit codes:
    0  success (ack received from UA)
    2  bad arguments
    3  network / rpc error
    4  grpcio missing and could not be installed
"""

import argparse
import os
import subprocess
import sys
import time


# --------------------------------------------------------------------------
# Ensure grpcio is available (one-shot pip install --user on first run).
# --------------------------------------------------------------------------

def _ensure_grpc():
    try:
        import grpc  # noqa: F401
        return
    except ImportError:
        pass

    print("grpcio not found, attempting `pip install --user grpcio` ...",
          file=sys.stderr)
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--user", "--quiet", "grpcio"],
            check=True,
        )
    except Exception as e:
        print(f"failed to install grpcio: {e}", file=sys.stderr)
        sys.exit(4)

    # Make sure --user site dir is on sys.path for the freshly-installed pkg.
    import site
    user_site = site.getusersitepackages()
    if user_site and user_site not in sys.path:
        sys.path.insert(0, user_site)


_ensure_grpc()
import grpc  # noqa: E402


# --------------------------------------------------------------------------
# Hand-rolled protobuf wire format for unified_agent.proto. We avoid pulling
# in grpcio-tools/protoc by encoding the handful of messages we need.
# --------------------------------------------------------------------------
# Wire types: 0 = varint, 2 = length-delimited (string/bytes/embedded-msg).

def _varint(v: int) -> bytes:
    out = bytearray()
    v &= (1 << 64) - 1
    while v > 0x7F:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)


def _tag(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)


def _length_delim(field: int, data: bytes) -> bytes:
    return _tag(field, 2) + _varint(len(data)) + data


def _str_field(field: int, s: str) -> bytes:
    return _length_delim(field, s.encode("utf-8"))


def _msg_field(field: int, inner: bytes) -> bytes:
    return _length_delim(field, inner)


def _packed_uint64(field: int, values) -> bytes:
    packed = b"".join(_varint(int(v)) for v in values)
    return _tag(field, 2) + _varint(len(packed)) + packed


def encode_session_meta_item(name: str, value: str) -> bytes:
    # SessionMetaItem: name=1, value=2
    return _str_field(1, name) + _str_field(2, value)


def encode_initialize(session_id: str, meta, shared_secret: str = "") -> bytes:
    # Initialize: session_id=1, meta=2 (repeated SessionMetaItem), shared_secret=3
    out = b""
    if session_id:
        out += _str_field(1, session_id)
    for (k, v) in meta:
        out += _msg_field(2, encode_session_meta_item(k, v))
    if shared_secret:
        out += _str_field(3, shared_secret)
    return out


def encode_request_initialize(session_id: str, meta, shared_secret: str = "") -> bytes:
    # Request.initialize (oneof field number = 1)
    return _msg_field(1, encode_initialize(session_id, meta, shared_secret))


def encode_databatch(seq_no, timestamp_us, payload: bytes) -> bytes:
    # DataBatch: seq_no=1 (repeated packed uint64),
    #            timestamp=2 (repeated packed uint64),
    #            payload=100 (repeated bytes),
    out = b""
    out += _packed_uint64(1, [seq_no])
    out += _packed_uint64(2, [timestamp_us])
    out += _length_delim(100, payload)
    return out


def encode_request_databatch(seq_no, timestamp_us, payload: bytes) -> bytes:
    # Request.data_batch (oneof field number = 2)
    return _msg_field(2, encode_databatch(seq_no, timestamp_us, payload))


# --------------------------------------------------------------------------
# gRPC client
# --------------------------------------------------------------------------

# gRPC service path is the same used by the official C++ client
# (library/cpp/unified_agent_client/client_impl.cpp::NewStub).
SERVICE_PATH = "/NUnifiedAgentProto.UnifiedAgentService/Session"


def send_annotation(uri: str, payload: bytes, meta, timeout: float = 10.0) -> bool:
    """Open a Session, do the full handshake, send one DataBatch, wait for Ack.

    Handshake (mirrors the C++ client in
    ``library/cpp/unified_agent_client/client_impl.cpp``):

        client -> Request.Initialize(meta)
        server -> Response.Initialized(session_id, last_seq_no)
        client -> Request.DataBatch(seq_no, timestamp_us, payload)
        server -> Response.Ack(seq_no)
        client -> half-close

    Returns True iff an Ack for seq_no=1 was received.
    """
    import queue

    channel = grpc.insecure_channel(uri)
    try:
        # Fail fast with a clear error when the TCP endpoint isn't listening,
        # instead of a confusing RPC-level error on first send.
        try:
            grpc.channel_ready_future(channel).result(timeout=max(1.0, timeout / 2))
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"cannot reach unified agent at {uri}: "
                f"channel not ready within {max(1.0, timeout/2):.1f}s"
            )

        stub = channel.stream_stream(
            SERVICE_PATH,
            request_serializer=lambda b: b,       # bytes in, bytes out
            response_deserializer=lambda b: b,
        )

        request_q: "queue.Queue[bytes]" = queue.Queue()
        sentinel = object()

        def request_iter():
            while True:
                item = request_q.get()
                if item is sentinel:
                    return
                yield item

        # Kick off the stream with Initialize.
        request_q.put(encode_request_initialize("", meta))

        responses = stub(request_iter(), timeout=timeout)

        got_init = False
        got_ack = False
        try:
            for resp in responses:
                if not resp:
                    continue
                # Response oneof field numbers: initialized=1, ack=2.
                field_number = resp[0] >> 3
                if field_number == 1 and not got_init:
                    got_init = True
                    # Send DataBatch only after Initialized, like the C++ client.
                    now_us = int(time.time() * 1_000_000)
                    request_q.put(
                        encode_request_databatch(1, now_us, payload)
                    )
                elif field_number == 2:
                    got_ack = True
                    break
        finally:
            # Unblock the request iterator so grpcio half-closes the send side
            # cleanly, letting the server finish on its own terms.
            request_q.put(sentinel)

        return got_ack
    finally:
        try:
            channel.close()
        except Exception:
            pass


def parse_uri(uri: str) -> str:
    if "://" in uri:
        uri = uri.split("://", 1)[1]
    if ":" not in uri:
        raise ValueError(f"invalid --uri {uri!r}; expected host:port")
    return uri


def main() -> int:
    p = argparse.ArgumentParser(
        description="Send one log record to a Unified Agent gRPC input",
    )
    p.add_argument("--uri", required=True,
                   help="Unified Agent gRPC uri (host:port)")
    p.add_argument("--message", required=True,
                   help="Log record text; becomes the DataBatch payload bytes")
    p.add_argument("--meta", action="append", default=[],
                   metavar="KEY=VALUE",
                   help="Session meta item (repeatable). "
                        "E.g. --meta cluster=nbs_load_cluster")
    p.add_argument("--timeout", type=float, default=10.0)
    args = p.parse_args()

    try:
        uri = parse_uri(args.uri)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 2

    meta = []
    for item in args.meta:
        if "=" not in item:
            print(f"invalid --meta {item!r}; expected KEY=VALUE", file=sys.stderr)
            return 2
        k, v = item.split("=", 1)
        meta.append((k, v))

    try:
        ok = send_annotation(
            uri, args.message.encode("utf-8"), meta, timeout=args.timeout,
        )
    except ConnectionError as e:
        print(f"ua_annotate: {e}", file=sys.stderr)
        return 3
    except grpc.RpcError as e:
        try:
            code = e.code()
            details = e.details()
        except Exception:
            code, details = "?", str(e)
        print(f"ua_annotate: grpc error talking to {uri}: "
              f"{code}: {details}", file=sys.stderr)
        return 3
    except Exception as e:
        print(f"ua_annotate: error: {e}", file=sys.stderr)
        return 3

    if not ok:
        print(f"ua_annotate: no ack received from UA at {uri}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
