# src/refining/pdf_to_text.py
# Recorre data/raw/pdfs/*.pdf -> genera data/interim/texts/*.txt con reflujo de columnas y limpieza.
# Requiere: PyMuPDF (pymupdf)

import argparse
import re
import sys
import fitz
from pathlib import Path
from typing import List, Tuple

HEADER_TOKENS = [
    "REPUBLICA ORIENTAL DEL URUGUAY", "REPÚBLICA ORIENTAL DEL URUGUAY",
    "DIARIO DE SESIONES", "COMISION PERMANENTE", "COMISIÓN PERMANENTE",
    "CÁMARA DE REPRESENTANTES", "CAMARA DE REPRESENTANTES",
    "CÁMARA DE SENADORES", "CAMARA DE SENADORES",
    "ASAMBLEA GENERAL",
    "XLII LEGISLATURA","XLIII LEGISLATURA","XLIV LEGISLATURA","XLV LEGISLATURA",
    "XLVI LEGISLATURA","XLVII LEGISLATURA","XLVIII LEGISLATURA","XLIX LEGISLATURA","L LEGISLATURA",
    "NÚMERO", "NUMERO"
]

# meses y días 
DATE_RE = re.compile(
    r"\b(lunes|martes|mi[eé]rcoles|jueves|viernes|s[áa]bado|domingo)\s+\d{1,2}\s+de\s+"
    r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|setiembre|septiembre|octubre|noviembre|diciembre)"
    r"(?:\s+de\s+\d{4})?\b",
    flags=re.IGNORECASE
)

CHAMBER_RE = re.compile(r"\bC[ÁA]MARA\s+DE\s+(REPRESENTANTES|SENADORES)\b", re.IGNORECASE)
LEGIS_RE = re.compile(r"\bL{1,3}X{0,3}V?I{0,3}\s+LEGISLATURA\b", re.IGNORECASE)  # tolerante

# Fecha sin día de la semana: "10 de noviembre de 2021"
DATE2_RE = re.compile(
    r"\b\d{1,2}\s+de\s+"
    r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|setiembre|septiembre|octubre|noviembre|diciembre)"
    r"\s+de\s+\d{4}\b",
    flags=re.IGNORECASE
)

# Variaciones de "NÚMERO/N.º/No."
NUM_LABEL_RE = re.compile(r"\b(N[úu]\.?m(?:ero|\.?)|N\.º|Nº|No\.?)\b", re.IGNORECASE)


def _looks_like_running_header(txt: str) -> bool:
    t = " ".join(txt.split())  # colapsa espacios/saltos
    if CHAMBER_RE.search(t) or DATE_RE.search(t) or DATE2_RE.search(t) or LEGIS_RE.search(t):
        return True
    if NUM_LABEL_RE.search(t):
        return True
    for tok in HEADER_TOKENS:
        if tok.lower() in t.lower():
            return True
    # Ej: "Miércoles 10 de noviembre de 2021 CÁMARA DE REPRESENTANTES 1"
    if re.search(r"\b\d{1,4}\b", t) and CHAMBER_RE.search(t):
        return True
    return False


def clean_text(raw: str) -> str:
    """
    Limpieza de texto para NLP:
    - Mantiene tildes y caracteres Unicode útiles (ñ, á, é, etc.)
    - Quita artefactos de OCR, headers, pies de página y espaciados raros
    - No usa unidecode (mantiene acentos)
    """

    # --- Normalización de caracteres invisibles / raros ---
    raw = raw.replace("\u00ad", "")   # soft hyphen
    raw = raw.replace("\ufeff", "")   # byte order mark
    raw = raw.replace("\u200b", "")   # zero width space
    raw = raw.replace("\xa0", " ")    # non-breaking space → normal space

    # --- Corrige palabras separadas por espacios espurios (ej: "C O M I S I Ó N") ---
    raw = re.sub(r"(?<=\w)\s(?=\w)(?:\s(?=\w))*", " ", raw)

    # --- Une palabras cortadas por guión de fin de línea ---
    raw = re.sub(r"(\w+)-\n(\w+)", r"\1\2", raw)

    # --- Une saltos de línea dentro de párrafos ---
    raw = re.sub(r"(?<![.!?;:–—\-])\n(?!\n|[•\-\u2022])", " ", raw)

    # --- Elimina múltiples espacios y saltos de línea excesivos ---
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)

    # --- Elimina líneas de pie de página (ej. "102 -C.P. COMISION PERMANENTE...") ---
    raw = re.sub(
        r"^\s*\d+\s*[-–—]?\s*C\.?P\.?.*$",
        "",
        raw,
        flags=re.MULTILINE | re.IGNORECASE,
    )

    # --- Elimina headers repetitivos (títulos fijos o números de página) ---
    def drop_header_lines(line: str) -> bool:
        t = line.strip()
        if not t:
            return True
        for token in HEADER_TOKENS:
            if token.lower() in t.lower():
                return True
        # números de página aislados
        if re.fullmatch(r"\d{1,4}", t):
            return True
        return False

    lines = raw.splitlines()
    pruned: List[str] = []
    for ln in lines:
        if drop_header_lines(ln):
            if ln.strip() == "":
                pruned.append("")
            continue
        pruned.append(ln)
    raw = "\n".join(pruned)

    # --- Espacios antes de puntuación ---
    raw = re.sub(r"\s+([,.;:!?])", r"\1", raw)
    raw = re.sub(r"[ \t]+\n", "\n", raw)

    # --- Limpieza final ---
    raw = raw.strip()
    # fuerza un solo salto final (útil si lo concatenás)
    if not raw.endswith("\n"):
        raw += "\n"

    return raw

def detect_split_x(blocks: List[Tuple[float, float, float, float, str]], page_width: float) -> float:
    """Devuelve x de corte entre columnas si parece haber 2 columnas; si no, -1."""
    # ignorar bloques demasiado anchos (cuelan en ambas columnas)
    narrow = []
    for b in blocks:
        x0, y0, x1, y1, txt = b
        width = x1 - x0
        if width < 0.7 * page_width and txt.strip():
            xm = (x0 + x1) / 2.0
            narrow.append((xm, b))
    if len(narrow) < 2:
        return -1

    narrow.sort(key=lambda t: t[0])
    gaps = []
    for i in range(len(narrow) - 1):
        gaps.append((narrow[i + 1][0] - narrow[i][0], i))
    if not gaps:
        return -1

    # mayor hueco en X
    best_gap, idx = max(gaps, key=lambda t: t[0])
    # si el gap es suficientemente grande, asumimos dos columnas
    if best_gap > 0.14 * page_width:
        left_mid = narrow[idx][0]
        right_mid = narrow[idx + 1][0]
        return (left_mid + right_mid) / 2.0
    return -1

def blocks_in_reading_order(blocks: List[Tuple[float, float, float, float, str]], split_x: float) -> List[Tuple[float, float, float, float, str]]:
    """Devuelve bloques en orden de lectura: primero col izq completa, luego col der."""
    if split_x <= 0:
        # orden simple por (y, x)
        return sorted(blocks, key=lambda b: (round(b[1], 1), round(b[0], 1)))

    left = []
    right = []
    middle = []
    for b in blocks:
        x0, y0, x1, y1, txt = b
        xm = (x0 + x1) / 2.0
        # bloques enormes a caballo de ambas columnas: los tratamos como "middle"
        if x0 < split_x < x1:
            middle.append(b)
        elif xm < split_x:
            left.append(b)
        else:
            right.append(b)

    left.sort(key=lambda b: (round(b[1], 1), round(b[0], 1)))
    right.sort(key=lambda b: (round(b[1], 1), round(b[0], 1)))
    middle.sort(key=lambda b: (round(b[1], 1), round(b[0], 1)))

    return left + middle + right

def extract_page_text(page, top_ratio=0.08, bottom_ratio=0.08) -> str:
    """Extrae texto de una página usando bloques, filtrando SOLO el encabezado superior y ordenando por columnas."""
    w = page.rect.width
    h = page.rect.height
    top_cut = h * top_ratio
    bot_cut = h * (1 - bottom_ratio)  # calculado pero NO usado para filtrar

    raw_blocks = page.get_text("blocks")  # (x0, y0, x1, y1, text, ...)
    blocks = []
    for b in raw_blocks:
        if len(b) < 5:
            continue
        x0, y0, x1, y1, txt = b[:5]
        if not txt or not txt.strip():
            continue

        # --- SOLO filtrar por banda superior (encabezado) ---
        if y1 < top_cut:
            continue

        # micro-ruido
        area = (x1 - x0) * (y1 - y0)
        if len(txt.strip()) <= 2 and area < (w * h * 0.002):
            continue

        # --- Heurística: SOLO arriba ---
        near_top = y0 < (h * 0.22)
        if near_top and _looks_like_running_header(txt):
            continue

        # encabezado ancho centrado arriba
        if (x1 - x0) > (0.75 * w) and y0 < (h * 0.30) and _looks_like_running_header(txt):
            continue

        blocks.append((x0, y0, x1, y1, txt))

    # --- Fallback: si nos quedamos sin bloques, reintentar sin heurísticas de encabezado
    if not blocks:
        for b in raw_blocks:
            if len(b) < 5:
                continue
            x0, y0, x1, y1, txt = b[:5]
            if not txt or not txt.strip():
                continue
            if y1 < top_cut:
                continue  # aún removemos la franja superior
            area = (x1 - x0) * (y1 - y0)
            if len(txt.strip()) <= 2 and area < (w * h * 0.002):
                continue
            blocks.append((x0, y0, x1, y1, txt))

    if not blocks:
        return ""

    split_x = detect_split_x(blocks, w)
    ordered = blocks_in_reading_order(blocks, split_x)

    # normaliza saltos y une bloques con doble newline
    pieces = []
    for x0, y0, x1, y1, txt in ordered:
        # borra números de página sueltos
        t = re.sub(r"^\s*\d{1,4}\s*$", "", txt.strip(), flags=re.MULTILINE)
        pieces.append(t.strip())

    page_text = "\n".join(p.strip() for p in pieces if p.strip())
    page_text = re.sub(r"\n{3,}", "\n\n", page_text)
    return page_text.strip()




def extract_pdf_text(pdf_path: Path, top_ratio=0.08, bottom_ratio=0.08) -> str:
    doc = fitz.open(pdf_path)
    pages = []
    for p in doc:
        pt = extract_page_text(p, top_ratio=top_ratio, bottom_ratio=bottom_ratio)
        if pt:
            pages.append(pt)
    raw = "\n\n".join(pages)
    return clean_text(raw)

def main():
    ap = argparse.ArgumentParser(description="Convierte PDFs de data/raw/pdfs a TXT en data/interim/texts con manejo de dos columnas.")
    ap.add_argument("--src", default="data/raw/pdfs", help="Directorio origen de PDFs (default: data/raw/pdfs)")
    ap.add_argument("--out", default="data/interim/texts", help="Directorio de salida TXT (default: data/interim/texts)")
    ap.add_argument("--overwrite", action="store_true", help="Sobrescribir si el TXT ya existe")
    ap.add_argument("--top", type=float, default=0.08, help="Ratio top para recorte de header (0-0.2 recomendado)")
    ap.add_argument("--bottom", type=float, default=0.08, help="Ratio bottom para recorte de footer (0-0.2 recomendado)")
    args = ap.parse_args()

    src_dir = Path(args.src)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(list(src_dir.glob("*.pdf")) + list(src_dir.glob("*.PDF")))
    if not pdfs:
        print(f"No encontré PDFs en {src_dir.resolve()}", file=sys.stderr)
        sys.exit(1)

    ok, bad = 0, 0
    #Limitar cantidad de pdfs convertidos para pruebas (Quitar cuando ya no sea necesario)
    #pdfs = pdfs[:5]
    for pdf in pdfs:
        out = out_dir / (pdf.stem + ".txt")
        if out.exists() and not args.overwrite:
            print(f"[SKIP] {pdf.name} -> {out.name} (existe)")
            ok += 1
            continue
        try:
            text = extract_pdf_text(pdf, top_ratio=args.top, bottom_ratio=args.bottom)
            out.write_text(text, encoding="utf-8")
            print(f"[OK]   {pdf.name} -> {out.name} ({len(text)} chars)")
            ok += 1
        except Exception as e:
            print(f"[FAIL] {pdf.name}: {e}", file=sys.stderr)
            bad += 1

    print(f"\nListo. OK={ok}  FAIL={bad}  Salida: {out_dir.resolve()}")

if __name__ == "__main__":
    main()
