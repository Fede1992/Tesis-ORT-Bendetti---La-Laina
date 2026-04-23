# src/tokenization/tokenizer.py
import json
import argparse
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from transformers import AutoTokenizer

def load_jsonl(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def chunk_text_to_max_tokens(
    text: str,
    tokenizer,
    max_len: int = 128,
) -> List[Tuple[str, int]]:
    """
    Parte 'text' en segmentos sin cortar palabras, cada uno con <= max_len tokens del vocab del tokenizer.
    Devuelve lista de (chunk_text, n_tokens).
    """
    # tokenizamos por palabras “suavemente” comprobando longitud de tokens en cada acumulación
    words = text.split()
    chunks: List[Tuple[str, int]] = []
    cur_words: List[str] = []

    def toks_len(s: str) -> int:
        # sin special tokens para medir puro contenido
        return len(tokenizer.encode(s, add_special_tokens=False, truncation=False))

    for w in words:
        candidate = ((" ".join(cur_words) + " " + w) if cur_words else w).strip()
        if toks_len(candidate) <= max_len:
            cur_words.append(w)
        else:
            if cur_words:
                s = " ".join(cur_words).strip()
                chunks.append((s, toks_len(s)))
                cur_words = [w]
            else:
                # palabra sola supera el límite (raro). hacemos fallback a recortar por subword
                # (pero preferimos no dejarla vacía)
                ids = tokenizer.encode(w, add_special_tokens=False)
                ids = ids[:max_len]
                s = tokenizer.decode(ids)
                chunks.append((s, len(ids)))
                cur_words = []

    if cur_words:
        s = " ".join(cur_words).strip()
        chunks.append((s, toks_len(s)))

    return chunks

def main():
    ap = argparse.ArgumentParser(
        description="Tokeniza df_final.jsonl en ventanas ≤N tokens sin cortar palabras (por defecto conserva metadata)."
    )
    ap.add_argument(
        "--in",
        dest="in_path",
        default="data/processed/df_final.jsonl",
        help="JSONL de entrada (default: data/processed/df_final.jsonl)",
    )
    ap.add_argument(
        "--out",
        dest="out_path",
        default="data/tokenized/rouberta_128.jsonl",
        help="JSONL de salida (default: data/tokenized/rouberta_128.jsonl)",
    )
    ap.add_argument(
        "--txt-out",
        dest="txt_out",
        default="",
        help="Ruta opcional para exportar SOLO texto (uno por línea) (default: vacío = no exporta)",
    )
    ap.add_argument(
        "--model",
        dest="model_name",
        default="roberta-base",
        help="Nombre del tokenizer HuggingFace (default: roberta-base). Pasá tu ROUBERTA acá si lo tenés publicado.",
    )
    ap.add_argument(
        "--max-len",
        dest="max_len",
        type=int,
        default=128,
        help="Máx. tokens por chunk (default: 128)",
    )
    # keep_meta = True por defecto; `--no-keep-meta` lo apaga
    ap.add_argument(
        "--no-keep-meta",
        dest="keep_meta",
        action="store_false",
        help="No incluir metadata en la salida (por defecto SÍ se incluye).",
    )
    ap.set_defaults(keep_meta=True)

    args = ap.parse_args()
    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    txt_out = Path(args.txt_out) if args.txt_out else None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if txt_out:
        txt_out.parent.mkdir(parents=True, exist_ok=True)

    tokenizer = AutoTokenizer.from_pretrained(args.model_name, use_fast=True)

    total_lines = 0
    total_chunks = 0

    with out_path.open("w", encoding="utf-8") as fout:
        ftxt = txt_out.open("w", encoding="utf-8") if txt_out else None
        try:
            for rec in load_jsonl(in_path):
                text = (rec.get("intervencion") or "").strip()
                if not text:
                    continue

                chunks = chunk_text_to_max_tokens(text, tokenizer, args.max_len)
                nchunks = len(chunks)
                for i, (chunk_text, ntoks) in enumerate(chunks):
                    if args.keep_meta:
                        out_obj = {
                            # metadata útil para trazabilidad
                            "file_name": rec.get("file_name"),
                            "cuerpo": rec.get("cuerpo"),
                            "fecha": rec.get("fecha"),
                            "locutor": rec.get("locutor"),
                            "encabezado": rec.get("encabezado"),
                            # payload para el modelo:
                            "text": chunk_text,
                            "n_tokens": ntoks,
                            "chunk_id": i + 1,
                            "chunk_count": nchunks,
                        }
                    else:
                        out_obj = {
                            "text": chunk_text,
                            "n_tokens": ntoks,
                            "chunk_id": i + 1,
                            "chunk_count": nchunks,
                        }

                    fout.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                    if ftxt:
                        ftxt.write(chunk_text.replace("\n", " ").strip() + "\n")

                    total_chunks += 1

                total_lines += 1

        finally:
            if ftxt:
                ftxt.close()

    print(f"Listo. Entradas procesadas: {total_lines}  |  Chunks generados: {total_chunks}")
    print(f"Salida JSONL: {out_path.resolve()}")
    if txt_out:
        print(f"Salida TXT (solo texto): {txt_out.resolve()}")

if __name__ == "__main__":
    main()
