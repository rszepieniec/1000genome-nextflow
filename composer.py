#!/usr/bin/env python3
"""
1000genome Composer dla Nextflow.

NL prompt -> ResearchIntent (REUSE llm_interpreter z HyperFlow composera)
          -> parametry pipeline'u -> nextflow run -> wyniki naukowe.

KLUCZOWY PUNKT: przednia polowa (NL -> intent) jest DOSLOWNIE zaimportowana
z pakietu workflow_composer (ten sam kod co napedza composer HyperFlow).
Nowy jest tylko backend: zamiast generowac workflow.json (HyperFlow),
generujemy parametry i wolamy `nextflow run` na naszym porcie 1000genome.

To realizuje teze: ten sam ResearchIntent -> dwa silniki -> te same wyniki.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ---- REUSE: przednia polowa composera HyperFlow (bez modyfikacji) ----
from workflow_composer.interpretation.llm_interpreter import (
    interpret_research_question,
    LLMConfig,
)
from workflow_composer.core.models import ResearchIntent

THIS_DIR = Path(__file__).parent.resolve()

def _find_nextflow():
    # 1) jawnie przez env; 2) wrapper obok (nextflow-experiments); 3) z PATH.
    env_bin = os.environ.get("NEXTFLOW_BIN")
    if env_bin:
        return env_bin
    wrapper = THIS_DIR.parent / "nextflow-experiments" / "bin" / "nextflow"
    if wrapper.exists():
        return str(wrapper)
    return shutil.which("nextflow") or str(wrapper)

NEXTFLOW_BIN = _find_nextflow()
NXF_VER = os.environ.get("NXF_VER", "25.10.2")

# Populacje dla ktorych mamy pliki (testdata/populations)
AVAILABLE_POPULATIONS = {"AFR", "ALL", "AMR", "EAS", "EUR", "GBR", "SAS"}


def intent_to_params(intent: ResearchIntent) -> dict:
    """Mapuje ResearchIntent -> parametry pipeline'u Nextflow.

    - POPULACJE z promptu -> parametr --populations (napedzaja mutation_overlap + frequency)
    - REGIONY z promptu   -> faza EXTRACT (tabix generuje dane z 1000 Genomes)
      Gdy prompt nie wskazuje regionu, uzywamy pre-wygenerowanych danych testowych.
    """
    pops = [p for p in intent.populations if p in AVAILABLE_POPULATIONS]
    if not pops:
        pops = ["GBR"]  # fallback
    dropped = [p for p in intent.populations if p not in AVAILABLE_POPULATIONS]

    # Regiony -> wiersze extract.csv: chrom,region,name
    extract_rows = []
    if intent.regions:
        for r in intent.regions:
            region_str = f"{r.chromosome}:{r.start}-{r.end}"
            extract_rows.append((r.chromosome, region_str, r.name.lower()))

    return {
        "populations": ",".join(pops),
        "dropped_populations": dropped,
        "extract_rows": extract_rows,
    }


def write_extract_csv(extract_rows: list, run_dir: Path) -> Path:
    csv_path = run_dir / "extract.csv"
    with open(csv_path, "w") as f:
        for chrom, region, name in extract_rows:
            f.write(f"{chrom},{region},{name}\n")
    return csv_path


def build_command(populations: str, run_dir: Path, extract_csv: Path | None,
                  max_variants: int = 0, n_runs: int = 0) -> list[str]:
    cmd = [
        str(NEXTFLOW_BIN),
        "run", str(THIS_DIR / "main.nf"),
        "--populations", populations,
        "--outdir", str(run_dir / "results"),
    ]
    if extract_csv is not None:
        cmd.extend(["--extract_csv", str(extract_csv)])
    if max_variants and int(max_variants) > 0:
        cmd.extend(["--max_variants", str(int(max_variants))])   # tryb szybki: limit wariantow
    if n_runs and int(n_runs) > 0:
        cmd.extend(["--n_runs", str(int(n_runs))])               # tryb szybki: mniej iteracji Monte Carlo
    return cmd


def main() -> int:
    p = argparse.ArgumentParser(description="1000genome Composer (Nextflow backend)")
    p.add_argument("prompt", help="Pytanie badawcze w jezyku naturalnym")
    p.add_argument("--model", default="gemini/gemini-2.5-flash", help="LLM (litellm)")
    p.add_argument("--dry-run", action="store_true", help="Tylko intent + komenda, bez uruchamiania")
    p.add_argument("--max-variants", type=int, default=0, help="TRYB SZYBKI: limit wariantow (0 = bez limitu)")
    p.add_argument("--n-runs", type=int, default=0, help="TRYB SZYBKI: iteracje Monte Carlo (0 = domyslne 1000)")
    args = p.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = THIS_DIR / "runs" / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"[composer] Run dir: {run_dir}")
    print(f"[composer] Prompt:  {args.prompt}")
    (run_dir / "prompt.txt").write_text(args.prompt)   # oryginalny prompt do historii w GUI

    # ---- FAZA 1: INTERPRET (REUSE) ----
    print(f"\n[composer] Faza 1: INTERPRET (reuse workflow_composer, model={args.model})")
    config = LLMConfig()
    config.model = args.model
    intent = interpret_research_question(args.prompt, config)
    (run_dir / "intent.json").write_text(intent.model_dump_json(indent=2))
    print(intent.model_dump_json(indent=2))

    # ---- FAZA 2: MAP intent -> params ----
    params = intent_to_params(intent)
    print(f"\n[composer] Faza 2: MAPOWANIE intent -> parametry Nextflow")
    print(f"  populacje: {params['populations']}")
    if params["dropped_populations"]:
        print(f"  ⚠️  pominiete (brak pliku populacji): {params['dropped_populations']}")

    extract_csv = None
    if params["extract_rows"]:
        extract_csv = write_extract_csv(params["extract_rows"], run_dir)
        regions_desc = ", ".join(f"{n} ({c}:{r.split(':')[1]})" for c, r, n in params["extract_rows"])
        print(f"  region(y) -> EXTRACT (tabix z 1000 Genomes): {regions_desc}")
    else:
        print(f"  brak regionu w promptcie -> dane testowe (chr17/BRCA1)")

    cmd = build_command(params["populations"], run_dir, extract_csv,
                        max_variants=args.max_variants, n_runs=args.n_runs)
    if args.max_variants or args.n_runs:
        print(f"[composer] TRYB SZYBKI: max_variants={args.max_variants or 'bez limitu'}, "
              f"n_runs={args.n_runs or 'domyslne 1000'}")
    (run_dir / "command.sh").write_text("#!/bin/bash\nNXF_VER=%s %s\n" % (NXF_VER, " ".join(cmd)))
    print(f"\n[composer] Komenda:\n  {' '.join(cmd)}")

    if args.dry_run:
        print("\n[composer] --dry-run -> koniec bez uruchamiania.")
        return 0

    # ---- FAZA 3: EXECUTE (Nextflow) ----
    print(f"\n[composer] Faza 3: EXECUTE (nextflow run)")
    log_path = run_dir / "nextflow.log"
    env = os.environ.copy()
    env["NXF_VER"] = NXF_VER
    print(f"[composer] Log na zywo -> {log_path}")
    with open(log_path, "w") as logf:
        rc = subprocess.run(cmd, stdout=logf, stderr=subprocess.STDOUT, env=env, cwd=str(run_dir)).returncode

    if rc == 0:
        print(f"\n[composer] SUKCES — wyniki:")
        for f in sorted((run_dir / "results").glob("*.tar.gz")):
            print(f"  {f.name}")
    else:
        print(f"\n[composer] FAIL (exit {rc}) — log: {log_path}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
