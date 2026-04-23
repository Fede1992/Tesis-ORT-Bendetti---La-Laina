# src/main.py
import os
import csv
import json
from typing import List

from src.settings.settings import load_settings
from src.settings.logger import custom_logger
from src.scrapers.parliament import ParliamentPDFScraper

logger = custom_logger("Main")

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def to_jsonl(rows: List[dict], path: str):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def to_csv(rows: List[dict], path: str):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    # --- Cargar config nueva ---
    general = load_settings("general")
    source  = load_settings("source")
    export  = load_settings("export")

    outdir = general.get("output_dir", "data")
    ensure_dir(outdir)

    # --- Inicializar y correr scraper de PDFs ---
    scraper = ParliamentPDFScraper()  # lee todo desde config.yml internamente
    rows = scraper.run()

    # --- Exportar resultados (metadata por PDF) ---
    interim_dir = os.path.join(outdir, "interim")
    ensure_dir(interim_dir)
    jsonl_path = os.path.join(interim_dir, export.get("jsonl_filename", "debates.jsonl"))
    csv_path   = os.path.join(interim_dir, export.get("csv_filename", "debates.csv"))

    to_jsonl(rows, jsonl_path)
    to_csv(rows, csv_path)

    logger.info(f"Listo. {len(rows)} documentos procesados.")
    logger.info(f"JSONL: {jsonl_path}")
    logger.info(f"CSV:   {csv_path}")
