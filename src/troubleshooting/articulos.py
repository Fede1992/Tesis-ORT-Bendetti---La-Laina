# src/troubleshooting/articulos.py
import argparse
import csv
from pathlib import Path
from typing import List, Tuple

# Importá tu extractor real
# Ajustá el import según dónde lo tengas definido
from src.refining.clean_texts import _extract_normative_blocks_anywhere


def summarize_file(
    path: Path,
    snippets_dir: Path,
    min_chars: int = 500,
    save_snippets: bool = True,
) -> dict:
    """
    Ejecuta el extractor en un archivo y devuelve métricas para el CSV.
    Opcionalmente guarda snippets por bloque para inspección visual.
    """
    text = path.read_text(encoding="utf-8", errors="ignore")
    cleaned, blocks = _extract_normative_blocks_anywhere(text, min_chars=min_chars)

    total_chars = len(text)
    cleaned_chars = len(cleaned)
    removed_chars = total_chars - cleaned_chars
    removed_ratio = (removed_chars / total_chars) if total_chars else 0.0

    # Guardar snippets por bloque
    per_block_rows = []
    if save_snippets and blocks:
        base = path.stem
        for i, b in enumerate(blocks):
            s, e = b.get("file_span", (0, 0))
            # contexto para ver si corta justo en medio de un discurso
            left = max(0, s - 300)
            right = min(len(text), e + 300)
            snippet = text[left:right]

            sn_file = snippets_dir / f"{base}__block{i:02d}.txt"
            sn_file.parent.mkdir(parents=True, exist_ok=True)
            sn_file.write_text(
                f"FILE: {path.name}\n"
                f"KIND: {b.get('kind')}\n"
                f"SPAN: {s}-{e} (len={len(b.get('text',''))})\n"
                f"N_ARTS: {b.get('n_articulos')}\n"
                f"HAS_MOTIVOS: {b.get('has_motivos')}\n"
                f"HAS_FIRMAS: {b.get('has_firmas')}\n"
                "----- CONTEXT START -----\n"
                + snippet +
                "\n----- CONTEXT END -----\n",
                encoding="utf-8"
            )

            per_block_rows.append({
                "file_name": path.name,
                "block_idx": i,
                "kind": b.get("kind"),
                "span_start": s,
                "span_end": e,
                "block_len": len(b.get("text", "")),
                "n_articulos": b.get("n_articulos"),
                "has_motivos": b.get("has_motivos"),
                "has_firmas": b.get("has_firmas"),
                "snippet_path": str(sn_file),
            })

    summary = {
        "file_name": path.name,
        "total_chars": total_chars,
        "cleaned_chars": cleaned_chars,
        "removed_chars": removed_chars,
        "removed_ratio": round(removed_ratio, 6),
        "n_blocks": len(blocks),
    }
    return summary, per_block_rows


def run(
    txt_dir: Path,
    glob: str,
    out_csv: Path,
    out_blocks_csv: Path,
    snippets_dir: Path,
    limit: int,
    min_chars: int,
) -> None:
    paths = sorted(txt_dir.glob(glob))
    if limit and limit > 0:
        paths = paths[:limit]

    summaries = []
    blocks_rows = []

    print(f"[i] Analizando {len(paths)} archivos de {txt_dir} con patrón '{glob}' "
          f"(min_chars={min_chars})")

    for p in paths:
        try:
            summary, per_block = summarize_file(
                p, snippets_dir=snippets_dir, min_chars=min_chars, save_snippets=True
            )
            summaries.append(summary)
            blocks_rows.extend(per_block)
            print(f" - {p.name}: removed={summary['removed_ratio']:.1%}, "
                  f"blocks={summary['n_blocks']}")
        except Exception as e:
            print(f"[!] Error en {p.name}: {e}")

    # CSV resumen por archivo
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "file_name", "total_chars", "cleaned_chars",
                "removed_chars", "removed_ratio", "n_blocks"
            ],
        )
        writer.writeheader()
        writer.writerows(summaries)

    # CSV detalle por bloque
    with out_blocks_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "file_name", "block_idx", "kind", "span_start", "span_end",
                "block_len", "n_articulos", "has_motivos", "has_firmas",
                "snippet_path",
            ],
        )
        writer.writeheader()
        writer.writerows(blocks_rows)

    print(f"[✓] Resumen por archivo: {out_csv}")
    print(f"[✓] Detalle por bloque:  {out_blocks_csv}")
    print(f"[✓] Snippets en:          {snippets_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Troubleshooting de recorte de normativa (bloques ARTÍCULOS/PROYECTO/MOTIVOS)."
    )
    parser.add_argument("--txt-dir", type=Path, default=Path("data/interim/txt_refined"),
                        help="Directorio con los .txt refinados")
    parser.add_argument("--glob", type=str, default="*crr*.txt",
                        help="Patrón de archivos a analizar (ej: '*crr*.txt' o '*.txt')")
    parser.add_argument("--out-csv", type=Path, default=Path("data/troubleshooting/articulos_report.csv"),
                        help="Salida CSV con resumen por archivo")
    parser.add_argument("--out-blocks-csv", type=Path, default=Path("data/troubleshooting/articulos_blocks.csv"),
                        help="Salida CSV con detalle por bloque")
    parser.add_argument("--snippets-dir", type=Path, default=Path("data/troubleshooting/snippets"),
                        help="Directorio donde guardar snippets de contexto por bloque")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limitar a N archivos (0 = sin límite)")
    parser.add_argument("--min-chars", type=int, default=500,
                        help="Umbral min de longitud para aceptar bloques (se pasa al extractor)")

    args = parser.parse_args()
    run(
        txt_dir=args.txt_dir,
        glob=args.glob,
        out_csv=args.out_csv,
        out_blocks_csv=args.out_blocks_csv,
        snippets_dir=args.snippets_dir,
        limit=args.limit,
        min_chars=args.min_chars,
    )


if __name__ == "__main__":
    main()
