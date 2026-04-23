#!/usr/bin/env python3
import sys, subprocess
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print("Uso: python -m src.troubleshooting.compare_last_two <CARPETA_FECHA> [--out OUT_DIR]", file=sys.stderr)
        sys.exit(2)
    folder = Path(sys.argv[1])
    out_dir = "troubleshooting/out"
    if "--out" in sys.argv:
        out_dir = sys.argv[sys.argv.index("--out") + 1]

    csvs = sorted(folder.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if len(csvs) < 2:
        print(f"No hay al menos 2 CSV en {folder}", file=sys.stderr)
        sys.exit(2)

    newest, previous = csvs[0], csvs[1]
    cmd = [
        sys.executable, "-m", "src.troubleshooting.compare_runs",
        str(previous), str(newest),
        "--labels", "prev", "curr",
        "--baseline", "prev",
        "--out-dir", out_dir
    ]
    print(">>", " ".join(cmd))
    sys.exit(subprocess.call(cmd))

if __name__ == "__main__":
    main()
