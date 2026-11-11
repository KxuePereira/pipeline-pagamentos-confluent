#!/usr/bin/env python3
"""
Confere os alertas que saíram do Flink contra o gabarito.

    python consumer/compare_alerts.py evidencias/04-alertas.jsonl              # seed + ao vivo
    python consumer/compare_alerts.py --ao-vivo evidencias/04-alertas-ao-vivo.jsonl

Gabarito = generator/expected_alerts_seed.txt (fraudes plantadas no seed)
         + evidencias/injecoes-fraude.jsonl (fraudes injetadas ao vivo, se houver).
Um alerta "bate" quando regra, cartão e IDs de transação são os mesmos.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def expected(only_live: bool) -> dict[tuple, str]:
    exp: dict[tuple, str] = {}
    seed = ROOT / "generator" / "expected_alerts_seed.txt"
    for line in ([] if only_live else seed.read_text(encoding="utf-8").splitlines()):
        m = re.match(r"(RULE_\w+)\s+card_id=(\d+).*transaction_ids=\[([\d, ]+)\]", line)
        if m:
            ids = tuple(sorted(int(x) for x in m.group(3).split(",")))
            exp[(m.group(1), int(m.group(2)), ids)] = "seed"
    live = ROOT / "evidencias" / "injecoes-fraude.jsonl"
    if live.exists():
        for line in live.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                exp[(r["regra_esperada"], r["card_id"], tuple(sorted(r["transaction_ids"])))] = "ao vivo"
    return exp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("arquivo", nargs="?", default=str(ROOT / "evidencias" / "04-alertas.jsonl"))
    ap.add_argument("--ao-vivo", action="store_true", help="compara só com as fraudes injetadas ao vivo")
    args = ap.parse_args()
    path = Path(args.arquivo)
    got: dict[tuple, dict] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        v = json.loads(line).get("value") or {}
        key = (v.get("rule_id"), v.get("card_id"), tuple(sorted(v.get("transaction_ids") or [])))
        got[key] = v

    exp = expected(args.ao_vivo)
    print(f"{'origem':8} {'regra':16} {'cartão':>6}  transações            resultado")
    for k, origem in sorted(exp.items(), key=lambda kv: (kv[1], kv[0][0], kv[0][1])):
        print(f"{origem:8} {k[0]:16} {k[1]:>6}  {str(list(k[2])):22} {'✔ alertado' if k in got else '✘ FALTOU'}")
    extras = [k for k in got if k not in exp]
    for k in extras:
        print(f"{'extra':8} {k[0]:16} {k[1]:>6}  {str(list(k[2])):22} ? não estava no gabarito")
    hit = sum(1 for k in exp if k in got)
    print(f"\n{hit}/{len(exp)} alertas esperados encontrados; {len(extras)} alerta(s) extra(s).")
    print("Obs.: alertas extras vindos do tráfego aleatório ao vivo são legítimos se a regra bate;"
          " uma rajada 'gasto alto' que cruza a borda da janela de 5 min pode não alertar (limite do TUMBLE).")


if __name__ == "__main__":
    main()
