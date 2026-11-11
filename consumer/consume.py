#!/usr/bin/env python3
"""
Consumidor Avro do pipeline — roda com a identidade <prefixo>-consumer.

Exemplos:
  # envelopes de CDC da conta da demo (insert/update/delete)
  python consumer/consume.py --topic desafio.public.accounts --match account_id=9001 \
      --out evidencias/03-envelopes-cdc.jsonl

  # alertas de fraude (fica ouvindo até 60 s sem mensagem nova)
  python consumer/consume.py --topic desafio.fraud.detected --timeout 60 \
      --out evidencias/04-alertas.jsonl

Lê endpoint e chaves de .state/ids.env e .state/secrets.env (criados pelo setup).
Cada execução usa um consumer group novo (prefixo <prefixo>-cli-), então sempre
lê o tópico desde o começo sem afetar outros consumidores.
"""
from __future__ import annotations

import argparse
import base64
import json
import shlex
import sys
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def read_state() -> dict[str, str]:
    cfg: dict[str, str] = {}
    for name in (".env", ".state/ids.env", ".state/secrets.env"):
        f = ROOT / name
        if not f.exists():
            continue
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                parts = shlex.split(v) if v else [""]
                cfg[k.strip()] = parts[0] if parts else ""
    return cfg


def to_json(o):
    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, bytes):
        return base64.b64encode(o).decode()
    return str(o)


def matches(key, value, conds: list[tuple[str, str]]) -> bool:
    if not conds:
        return True
    pools = [key if isinstance(key, dict) else {}]
    if isinstance(value, dict):
        pools += [value, value.get("before") or {}, value.get("after") or {}]
    return all(any(str(p.get(k)) == v for p in pools if isinstance(p, dict)) for k, v in conds)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--timeout", type=int, default=30, help="para após N s sem mensagem nova")
    ap.add_argument("--max", type=int, default=0, help="para após N mensagens (0 = sem limite)")
    ap.add_argument("--match", action="append", default=[], metavar="CAMPO=VALOR",
                    help="filtra por campo da chave, do valor, do before ou do after")
    ap.add_argument("--out", help="anexa cada mensagem como JSON Lines neste arquivo")
    ap.add_argument("--latest", action="store_true", help="só mensagens novas (padrão: desde o início)")
    args = ap.parse_args()

    from confluent_kafka import Consumer, KafkaError
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    cfg = read_state()
    need = ["BOOTSTRAP", "CONSUMER_KAFKA_KEY", "CONSUMER_KAFKA_SECRET", "SR_URL", "CONSUMER_SR_KEY", "CONSUMER_SR_SECRET"]
    missing = [k for k in need if not cfg.get(k)]
    if missing:
        sys.exit(f"Faltam {missing} em .state/ — rode ./setup.sh 1 antes.")

    prefix = cfg.get("CC_PREFIX", "desafio")
    sr = SchemaRegistryClient({"url": cfg["SR_URL"],
                               "basic.auth.user.info": f"{cfg['CONSUMER_SR_KEY']}:{cfg['CONSUMER_SR_SECRET']}"})
    deser = AvroDeserializer(sr)
    consumer = Consumer({
        "bootstrap.servers": cfg["BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": cfg["CONSUMER_KAFKA_KEY"],
        "sasl.password": cfg["CONSUMER_KAFKA_SECRET"],
        "group.id": f"{prefix}-cli-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest" if args.latest else "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])
    conds = [tuple(m.split("=", 1)) for m in args.match]
    out = open(args.out, "a", encoding="utf-8") if args.out else None

    seen = printed = 0
    last = time.monotonic()
    print(f"Lendo {args.topic} (para após {args.timeout}s sem mensagens)...", file=sys.stderr)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last > args.timeout:
                    break
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"erro: {msg.error()}", file=sys.stderr)
                continue
            last = time.monotonic()
            seen += 1
            ctx_k = SerializationContext(msg.topic(), MessageField.KEY)
            ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
            try:
                key = deser(msg.key(), ctx_k) if msg.key() else None
            except Exception:  # chave não-Avro
                key = msg.key().decode(errors="replace")
            value = deser(msg.value(), ctx_v) if msg.value() is not None else None
            if not matches(key, value, conds):
                continue

            rec = {"offset": msg.offset(), "kafka_ts": msg.timestamp()[1], "key": key}
            if isinstance(value, dict) and "op" in value and "after" in value:   # envelope Debezium
                rec.update(op=value["op"], before=value.get("before"), after=value.get("after"),
                           ts_ms=value.get("ts_ms"))
            elif value is None:
                rec["value"] = None
                rec["nota"] = "tombstone (valor nulo): sinal de compactação depois do delete"
            else:
                rec["value"] = value
            line = json.dumps(rec, default=to_json, ensure_ascii=False)
            print(line)
            if out:
                out.write(line + "\n")
            printed += 1
            if args.max and printed >= args.max:
                break
    finally:
        consumer.close()
        if out:
            out.close()
    print(f"{printed} mensagem(ns) exibida(s) de {seen} lida(s).", file=sys.stderr)


if __name__ == "__main__":
    main()
