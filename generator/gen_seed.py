#!/usr/bin/env python3
"""
Gera o seed determinístico do pipeline (seed=42) em SQL puro.

    python generator/gen_seed.py                # escreve sql/postgres/02_seed.sql
    python generator/gen_seed.py --check        # só mostra os alertas esperados

Não precisa de banco nem de bibliotecas externas: o mesmo seed gera sempre
os mesmos 200 clientes, 240 contas, 320 cartões, 60 lojas e 2000 transações.

O seed já vem com fraudes "plantadas" no histórico. Assim, assim que o Flink
sobe, ele precisa encontrar exatamente os alertas listados em
generator/expected_alerts_seed.txt. É a prova de que a regra funciona,
sem depender de sorte.
"""
from __future__ import annotations

import argparse
import math
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

SEED = 42
N_CUSTOMERS, N_ACCOUNTS, N_CARDS, N_MERCHANTS, N_TRANSACTIONS = 200, 240, 320, 60, 2000

ACCOUNTS_BASE = datetime(2026, 10, 1, 0, 0, 0)
TX_BASE = datetime(2026, 11, 1, 0, 0, 0)
TX_SPAN = timedelta(days=7)

# Regras (iguais às do Flink SQL — se mudar aqui, mude lá)
VELOCITY_COUNT, VELOCITY_WINDOW_S = 3, 60
HIGHSPEND_WINDOW_MIN, HIGHSPEND_RATIO, HIGHSPEND_MIN_TX = 5, Decimal("0.5"), 2

ROOT = Path(__file__).resolve().parent.parent

FIRST = ["Ana", "Bruno", "Camila", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela",
         "João", "Karina", "Lucas", "Mariana", "Nicolas", "Olívia", "Pedro", "Rafaela", "Samuel",
         "Tatiane", "Vitor", "Yasmin", "Thiago", "Larissa", "Gustavo", "Beatriz", "Rodrigo"]
LAST = ["Silva", "Santos", "Oliveira", "Souza", "Pereira", "Lima", "Carvalho", "Ferreira",
        "Rodrigues", "Almeida", "Costa", "Gomes", "Martins", "Araújo", "Barbosa", "Ribeiro"]
CITIES = [("São Paulo", "SP"), ("Campinas", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
          ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"), ("Recife", "PE"),
          ("Fortaleza", "CE"), ("Brasília", "DF"), ("Goiânia", "GO"), ("Florianópolis", "SC")]
MERCHANT_KINDS = [  # (mcc, categoria, nomes)
    ("5411", "SUPERMERCADO", ["Mercado Bom Preço", "Super Econômico", "Hiper Família"]),
    ("5812", "RESTAURANTE", ["Cantina da Nonna", "Sabor Mineiro", "Sushi Prime"]),
    ("5541", "POSTO", ["Auto Posto Rota", "Posto Estrela"]),
    ("5912", "FARMACIA", ["Drogaria Saúde", "Farma Vida"]),
    ("5732", "ELETRONICOS", ["Tech Center", "Loja Digital", "Mega Eletro"]),
    ("4121", "TRANSPORTE", ["Corrida Já", "Táxi Expresso"]),
    ("5999", "ECOMMERCE", ["Marketplace Online", "Loja Virtual Plus", "Compra Fácil"]),
    ("7011", "HOTEL", ["Hotel Central", "Pousada do Mar"]),
]
BRANDS = ["VISA", "MASTERCARD", "ELO"]
CHANNELS = (["POS", "ECOMMERCE", "CONTACTLESS"], [0.45, 0.30, 0.25])


@dataclass
class Tx:
    created_at: datetime
    card_id: int
    account_id: int
    merchant_id: int
    amount: Decimal
    channel: str
    status: str
    planted: str = ""


def money(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def ts(d: datetime) -> str:
    return q(d.strftime("%Y-%m-%d %H:%M:%S.") + f"{d.microsecond // 1000:03d}")


def slug(s: str) -> str:
    table = str.maketrans("áàâãéêíóôõúçÁÉÍÓÚÇ", "aaaaeeiooouc" + "AEIOUC")
    return s.translate(table).lower().replace(" ", ".")


def build(rng: random.Random):
    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        city, uf = rng.choice(CITIES)
        email = f"{slug(name)}.{cid}@exemplo.com.br"
        created = ACCOUNTS_BASE - timedelta(days=30) + timedelta(minutes=cid)
        customers.append((cid, name, email, city, uf, created))

    accounts = []
    for aid in range(1, N_ACCOUNTS + 1):
        cust = aid if aid <= N_CUSTOMERS else rng.randint(1, N_CUSTOMERS)
        status = "BLOCKED" if rng.random() < 0.04 else "ACTIVE"
        limit = money(rng.choice([1000, 2000, 3000, 5000, 8000, 10000, 15000, 20000]))
        tier = rng.choices(["LOW", "MEDIUM", "HIGH"], [0.6, 0.3, 0.1])[0]
        accounts.append((aid, cust, status, limit, tier, ACCOUNTS_BASE + timedelta(minutes=aid)))
    acc_by_id = {a[0]: a for a in accounts}

    cards = []
    for kid in range(1, N_CARDS + 1):
        aid = kid if kid <= N_ACCOUNTS else rng.randint(1, N_ACCOUNTS)
        last4 = f"{rng.randint(0, 9999):04d}"
        cards.append((kid, aid, last4, rng.choice(BRANDS), "ACTIVE", ACCOUNTS_BASE + timedelta(minutes=kid)))

    merchants = []
    for mid in range(1, N_MERCHANTS + 1):
        mcc, cat, names = MERCHANT_KINDS[(mid - 1) % len(MERCHANT_KINDS)]
        city, _uf = rng.choice(CITIES)
        country = "BR" if rng.random() < 0.9 else rng.choice(["US", "PT", "AR"])
        merchants.append((mid, f"{rng.choice(names)} #{mid}", mcc, cat, city, country,
                          ACCOUNTS_BASE + timedelta(minutes=mid)))

    usable_cards = [c for c in cards if acc_by_id[c[1]][2] == "ACTIVE"]

    # ---- Fraudes plantadas (ground truth) -------------------------------
    planted: list[Tx] = []

    def plant(card, start: datetime, offsets_s: list[int], amounts: list[float], tag: str):
        for off, amt in zip(offsets_s, amounts):
            planted.append(Tx(start + timedelta(seconds=off), card[0], card[1],
                              rng.randint(1, N_MERCHANTS), money(amt), "ECOMMERCE", "APPROVED", tag))

    c1, c2, c3 = rng.sample(usable_cards, 3)
    # 3 compras em 31 s no mesmo cartão -> RULE_VELOCITY
    plant(c1, TX_BASE + timedelta(days=2, hours=3, minutes=7, seconds=5), [0, 12, 31],
          [89.90, 149.90, 249.90], "velocity-3x")
    # 4 compras em 50 s -> RULE_VELOCITY dispara 1 vez (a 4ª sobra, SKIP PAST LAST ROW)
    plant(c2, TX_BASE + timedelta(days=4, hours=21, minutes=40, seconds=0), [0, 9, 22, 50],
          [35.00, 42.50, 58.10, 61.00], "velocity-4x")
    # 2 compras = 60% do limite dentro da mesma janela de 5 min -> RULE_HIGH_SPEND
    lim = float(acc_by_id[c3[1]][3])
    window_start = TX_BASE + timedelta(days=5, hours=14, minutes=10)
    plant(c3, window_start + timedelta(seconds=60), [0, 40], [lim * 0.30, lim * 0.30], "high-spend")

    # ---- Transações "normais" -------------------------------------------
    n_bg = N_TRANSACTIONS - len(planted)
    offsets = sorted(rng.uniform(0, TX_SPAN.total_seconds()) for _ in range(n_bg))
    background: list[Tx] = []
    for off in offsets:
        card = rng.choice(usable_cards)
        amount = money(min(max(rng.lognormvariate(math.log(80), 0.9), 5), 3000))
        background.append(Tx(TX_BASE + timedelta(seconds=round(off, 3)), card[0], card[1],
                             rng.randint(1, N_MERCHANTS), amount,
                             rng.choices(*CHANNELS)[0],
                             "APPROVED" if rng.random() < 0.97 else "DECLINED"))

    txs = sorted(background + planted, key=lambda t: (t.created_at, t.card_id))
    return customers, accounts, cards, merchants, txs, acc_by_id


def expected_alerts(txs: list[Tx], acc_by_id) -> list[str]:
    """Reproduz as duas regras do Flink em Python (ground truth)."""
    out = []
    by_card: dict[int, list[tuple[int, Tx]]] = defaultdict(list)
    for i, t in enumerate(txs, start=1):
        by_card[t.card_id].append((i, t))

    # RULE_VELOCITY: PATTERN (T{3}) WITHIN 60 s, AFTER MATCH SKIP PAST LAST ROW
    for card, rows in sorted(by_card.items()):
        i = 0
        while i + VELOCITY_COUNT - 1 < len(rows):
            first, last = rows[i], rows[i + VELOCITY_COUNT - 1]
            if (last[1].created_at - first[1].created_at).total_seconds() < VELOCITY_WINDOW_S:
                ids = [r[0] for r in rows[i:i + VELOCITY_COUNT]]
                out.append(f"RULE_VELOCITY   card_id={card} transaction_ids={ids}")
                i += VELOCITY_COUNT
            else:
                i += 1

    # RULE_HIGH_SPEND: TUMBLE 5 min, 2+ compras somando > 50% do limite da conta
    buckets: dict[tuple[int, datetime], list[tuple[int, Tx]]] = defaultdict(list)
    for i, t in enumerate(txs, start=1):
        w = t.created_at.replace(second=0, microsecond=0)
        w -= timedelta(minutes=w.minute % HIGHSPEND_WINDOW_MIN)
        buckets[(t.card_id, w)].append((i, t))
    for (card, w), rows in sorted(buckets.items()):
        total = sum(r[1].amount for r in rows)
        limit = acc_by_id[rows[0][1].account_id][3]
        if len(rows) >= HIGHSPEND_MIN_TX and total > limit * HIGHSPEND_RATIO:
            out.append(f"RULE_HIGH_SPEND card_id={card} window_start={w:%Y-%m-%d %H:%M} "
                       f"total={total} limite={limit} transaction_ids={[r[0] for r in rows]}")
    return out


def render_sql(customers, accounts, cards, merchants, txs) -> str:
    lines = [
        "-- =====================================================================",
        f"-- 02_seed.sql  —  GERADO por generator/gen_seed.py (seed={SEED}). Não edite à mão.",
        f"-- customers={len(customers)} accounts={len(accounts)} cards={len(cards)} "
        f"merchants={len(merchants)} transactions={len(txs)}",
        "-- Rode ANTES de ligar o conector (o snapshot inicial leva tudo isso).",
        "-- =====================================================================",
        "BEGIN;",
        "TRUNCATE transactions, cards, merchants, accounts, customers RESTART IDENTITY CASCADE;",
        "",
    ]

    def bulk(table: str, cols: str, rows: list[str], size: int = 500):
        for i in range(0, len(rows), size):
            lines.append(f"INSERT INTO {table} ({cols}) VALUES")
            lines.append(",\n".join(rows[i:i + size]) + ";")
        lines.append("")

    bulk("customers", "customer_id, full_name, email, city, state, created_at, updated_at",
         [f"({c[0]}, {q(c[1])}, {q(c[2])}, {q(c[3])}, {q(c[4])}, {ts(c[5])}, {ts(c[5])})" for c in customers])
    bulk("accounts", "account_id, customer_id, status, credit_limit, risk_tier, created_at, updated_at",
         [f"({a[0]}, {a[1]}, {q(a[2])}, {a[3]}, {q(a[4])}, {ts(a[5])}, {ts(a[5])})" for a in accounts])
    bulk("cards", "card_id, account_id, pan_last4, brand, status, created_at, updated_at",
         [f"({k[0]}, {k[1]}, {q(k[2])}, {q(k[3])}, {q(k[4])}, {ts(k[5])}, {ts(k[5])})" for k in cards])
    bulk("merchants", "merchant_id, name, mcc, category, city, country, created_at, updated_at",
         [f"({m[0]}, {q(m[1])}, {q(m[2])}, {q(m[3])}, {q(m[4])}, {q(m[5])}, {ts(m[6])}, {ts(m[6])})"
          for m in merchants])
    bulk("transactions",
         "transaction_id, card_id, account_id, merchant_id, amount, currency, channel, status, created_at",
         [f"({i}, {t.card_id}, {t.account_id}, {t.merchant_id}, {t.amount}, 'BRL', {q(t.channel)}, "
          f"{q(t.status)}, {ts(t.created_at)})" for i, t in enumerate(txs, start=1)])

    lines += [
        "-- a identidade continua de onde o seed parou (o gerador ao vivo começa em 2001)",
        "SELECT setval(pg_get_serial_sequence('transactions', 'transaction_id'), "
        "(SELECT max(transaction_id) FROM transactions));",
        "COMMIT;",
        "",
        "SELECT 'customers' AS tabela, count(*) FROM customers UNION ALL",
        "SELECT 'accounts', count(*) FROM accounts UNION ALL",
        "SELECT 'cards', count(*) FROM cards UNION ALL",
        "SELECT 'merchants', count(*) FROM merchants UNION ALL",
        "SELECT 'transactions', count(*) FROM transactions;",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="só imprime os alertas esperados")
    args = ap.parse_args()

    customers, accounts, cards, merchants, txs, acc_by_id = build(random.Random(SEED))
    alerts = expected_alerts(txs, acc_by_id)
    report = "\n".join([f"# Alertas esperados no seed (seed={SEED}) — gerado por gen_seed.py", *alerts, ""])

    if not args.check:
        (ROOT / "sql/postgres/02_seed.sql").write_text(render_sql(customers, accounts, cards, merchants, txs),
                                                      encoding="utf-8")
        (ROOT / "generator/expected_alerts_seed.txt").write_text(report, encoding="utf-8")
        print("ok: sql/postgres/02_seed.sql e generator/expected_alerts_seed.txt")
    print(report)


if __name__ == "__main__":
    main()
