#!/usr/bin/env python3
"""
Gerador ao vivo: insere transações no Postgres para o CDC levar até o Flink.

    python generator/stream_transactions.py                  # roda até Ctrl+C
    python generator/stream_transactions.py --duration 300   # 5 minutos

O que ele faz, num único loop:
  * tráfego normal: ~1 transação/s em cartões aleatórios (mantém o watermark
    do Flink andando, senão a última janela nunca fecha);
  * a cada --fraud-every s: 3 compras no MESMO cartão em ~10 s  -> RULE_VELOCITY;
  * a cada --highspend-every s: 2 compras espaçadas de 20 s somando 60% do
    limite da conta -> RULE_HIGH_SPEND.

Cada fraude injetada é registrada em evidencias/injecoes-fraude.jsonl para você
comparar com os alertas que saem do Flink (o "gabarito" da execução ao vivo).

Lê DATABASE_URL do ambiente (ou do .env na raiz do repositório).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / "evidencias" / "injecoes-fraude.jsonl"

INSERT = """
INSERT INTO transactions (card_id, account_id, merchant_id, amount, currency, channel, status)
VALUES (%s, %s, %s, %s, 'BRL', %s, 'APPROVED')
RETURNING transaction_id, created_at
"""


def load_dotenv() -> None:
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def money(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class Generator:
    def __init__(self, conn, rng: random.Random):
        self.conn, self.rng = conn, rng
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.card_id, c.account_id, a.credit_limit
                FROM cards c JOIN accounts a USING (account_id)
                WHERE c.status = 'ACTIVE' AND a.status = 'ACTIVE'
                ORDER BY c.card_id""")
            self.cards = cur.fetchall()
            cur.execute("SELECT merchant_id FROM merchants ORDER BY merchant_id")
            self.merchants = [r[0] for r in cur.fetchall()]
        if not self.cards or not self.merchants:
            sys.exit("Banco sem cartões/lojas. Rode antes sql/postgres/02_seed.sql.")
        self.count = 0

    def insert(self, card, amount: Decimal, channel: str | None = None):
        channel = channel or self.rng.choices(["POS", "ECOMMERCE", "CONTACTLESS"], [0.45, 0.30, 0.25])[0]
        with self.conn.cursor() as cur:
            cur.execute(INSERT, (card[0], card[1], self.rng.choice(self.merchants), amount, channel))
            tx_id, created = cur.fetchone()
        self.conn.commit()
        self.count += 1
        return tx_id, created

    def normal(self):
        card = self.rng.choice(self.cards)
        amount = money(min(max(self.rng.lognormvariate(4.38, 0.8), 5), 800))
        return self.insert(card, amount)

    @staticmethod
    def log(kind: str, card, rows) -> None:
        rec = {
            "injetado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "regra_esperada": kind,
            "card_id": card[0],
            "account_id": card[1],
            "transaction_ids": [r[0] for r in rows],
            "created_at": [r[1].isoformat(timespec="milliseconds") for r in rows],
        }
        EVIDENCE.parent.mkdir(exist_ok=True)
        with EVIDENCE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"[FRAUDE INJETADA] {kind} card_id={card[0]} tx={rec['transaction_ids']}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=float, default=1.0, help="transações normais por segundo (padrão 1)")
    ap.add_argument("--fraud-every", type=int, default=45, help="segundos entre rajadas de velocidade")
    ap.add_argument("--highspend-every", type=int, default=150, help="segundos entre gastos altos (0 desliga)")
    ap.add_argument("--duration", type=int, default=0, help="segundos de execução (0 = até Ctrl+C)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    load_dotenv()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        sys.exit("Defina DATABASE_URL (veja .env.example).")

    import psycopg  # import tardio: --help funciona sem a lib instalada

    rng = random.Random(args.seed)
    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))

    with psycopg.connect(dsn, autocommit=False) as conn:
        gen = Generator(conn, rng)
        print(f"Gerando: {args.rate}/s normal, velocidade a cada {args.fraud_every}s, "
              f"gasto alto a cada {args.highspend_every}s. Ctrl+C para parar.", flush=True)
        start = time.monotonic()
        next_fraud = start + 15                       # primeira rajada logo no começo
        next_high = start + 30 if args.highspend_every else float("inf")
        pending: list = []                            # passos agendados da rajada em curso

        while not stop["now"]:
            now = time.monotonic()
            if args.duration and now - start >= args.duration:
                break

            if now >= next_fraud and not pending:
                card = rng.choice(gen.cards)
                pending = [("RULE_VELOCITY", card, now + d, money(rng.uniform(40, 300)), [])
                           for d in (0, 4, 9)]
                next_fraud = now + args.fraud_every
            if now >= next_high and not pending:
                card = rng.choice(gen.cards)
                part = money(float(card[2]) * 0.30)
                pending = [("RULE_HIGH_SPEND", card, now + d, part, []) for d in (0, 20)]
                next_high = now + args.highspend_every

            if pending and now >= pending[0][2]:
                kind, card, _, amount, _ = pending[0]
                row = gen.insert(card, amount, "ECOMMERCE")
                done = pending[0][4] + [row]
                pending.pop(0)
                if pending:
                    pending = [(k, c, t, a, done) for (k, c, t, a, _) in pending]
                else:
                    gen.log(kind, card, done)
            else:
                gen.normal()
                if gen.count % 30 == 0:
                    print(f"  {gen.count} transações inseridas", flush=True)

            time.sleep(1.0 / args.rate if args.rate > 0 else 1.0)

    print(f"Fim. {gen.count} transações inseridas.")


if __name__ == "__main__":
    main()
