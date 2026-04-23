import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ----------------------------
# Oradores (terminan en ".-" o ":")
# ----------------------------
SPEAKER_HEADER_RE = re.compile(
    r"(?m)^(?P<header>"
    r"(?:SEÑOR(?:A)?)\s+(?:PRESIDENTE|PRESIDENTA)(?:\s*\([^)]+\))?"
    r"|"
    r"(?:SEÑOR(?:A)?)\s+[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s\.'-]{0,80}"
    r")\s*(?:\.-|:)\s"
)

# ----------------------------
# Encabezados / títulos de secciones
# ----------------------------
LINE_IS_ALLCAPS_RE = re.compile(r"^[A-ZÁÉÍÓÚÜÑ0-9 ,;:()\-–\.]{3,}$")
NUM_HEADER_RE      = re.compile(r"^\s*(\d{1,2})\)\s+(.+)$", flags=re.UNICODE)

BAD_HEADER_PREFIXES = tuple(s.lower() for s in [
    "sumario", "preside la", "sesión", "sesion", "páginas", "paginas",
    "texto de la citación", "texto de la citacion"
])

TIENE_LA_PALABRA_RE = re.compile(r"\bTiene la palabra\b.*$", flags=re.IGNORECASE)
PAG_TOKEN_RE = re.compile(r"\bPÁG\.|\bPAG\.", flags=re.IGNORECASE)
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

def strip_control_chars(s: str) -> str:
    return CONTROL_CHARS_RE.sub("", s)

def clamp_words(s: str, max_words: int = 24) -> str:
    parts = s.strip().split()
    return " ".join(parts[:max_words]).strip()

def likely_index_dots(s: str) -> bool:
    return s.count(".") > 5 and " " in s

def is_good_header_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if likely_index_dots(s):
        return False
    low = s.lower()
    if any(low.startswith(pref) for pref in BAD_HEADER_PREFIXES):
        return False
    if NUM_HEADER_RE.match(s):
        return True
    if LINE_IS_ALLCAPS_RE.match(s) and 4 <= len(s) <= 140:
        return True
    return False

def normalize_header_text(h: str, max_words: int = 24) -> str:
    h = h.strip()
    h = PAG_TOKEN_RE.split(h)[0].strip()
    h = TIENE_LA_PALABRA_RE.sub("", h).strip()
    h = re.sub(r"\s+", " ", h)
    h = re.sub(r"\s*\.+$", "", h)

    m = NUM_HEADER_RE.match(h)
    if m:
        num, title = m.groups()
        title = title.strip()
        if re.match(r"(?i)^asistencia\b", title):
            title = "ASISTENCIA"
        elif re.match(r"(?i)^asuntos\s+entrados\b", title):
            title = "ASUNTOS ENTRADOS"
        else:
            title = title.upper()
        title = clamp_words(title, max_words)
        return f"{num}) {title}"

    h = h.upper()
    h = clamp_words(h, max_words)
    return h

def scan_headers_joining(text: str) -> List[Tuple[int, str]]:
    lines = text.splitlines()
    positions = []
    pos = 0
    for ln in lines:
        positions.append(pos)
        pos += len(ln) + 1

    headers: List[Tuple[int, str]] = []
    i = 0
    while i < len(lines):
        ln = lines[i].strip()
        pos_i = positions[i]
        m = NUM_HEADER_RE.match(ln)
        if m and is_good_header_line(ln):
            to_join = ln
            if i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if (LINE_IS_ALLCAPS_RE.match(nxt)
                    and not likely_index_dots(nxt)
                    and not TIENE_LA_PALABRA_RE.search(nxt)):
                    to_join = f"{ln} {nxt}"
                    i += 1
            headers.append((pos_i, normalize_header_text(to_join)))
            i += 1
            continue

        if is_good_header_line(ln):
            if ln.startswith("SEÑOR") or ln.startswith("SEÑORA"):
                i += 1
                continue
            headers.append((pos_i, normalize_header_text(ln)))
            i += 1
            continue

        i += 1

    dedup = []
    last = None
    for p, h in headers:
        if h != last:
            dedup.append((p, h))
            last = h
    return dedup

def closest_prev_header(pos: int, headers: List[Tuple[int, str]], fallback: Optional[str] = None) -> Optional[str]:
    prev = [h for (p, h) in headers if p < pos]
    return prev[-1] if prev else (normalize_header_text(fallback) if fallback else None)

# ----------------------------
# Limpieza de intervención
# ----------------------------
STAGE_PARENS_RE = re.compile(r"\([^)]*\)")
LINK_RE = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)

def clean_intervention_text(s: str) -> str:
    s = LINK_RE.sub("<link>", s)
    s = STAGE_PARENS_RE.sub(" ", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s+\n", "\n", s)
    return s.strip()

def parse_locutor(speaker_header: str) -> str:
    return re.sub(r"(?:\.-|:)$", "", speaker_header).strip()

# ----------------------------
# Meta desde filename
# ----------------------------
KNOWN_SIGLAS = {"crr", "c.p.", "css", "a.g."}

def infer_meta_from_filename(stem: str) -> Dict[str, Optional[str]]:
    n_leg = None; fecha = None; cuerpo = None
    m = re.match(r"^\s*(\d{1,3})_", stem)
    if m:
        try:
            n_leg = int(m.group(1))
        except:
            pass

    mf = re.search(r"(20\d{2})[-_\.](\d{2})[-_\.](\d{2})", stem)
    if mf:
        fecha = f"{mf.group(1)}-{mf.group(2)}-{mf.group(3)}"

    tokens = re.findall(r"(?<=_)([A-Za-z\.]{2,5})(?=_|$)", stem)
    candidates = []
    for i, t in enumerate(tokens):
        tt = t.lower()
        if not re.fullmatch(r"[a-z\.]{2,5}", tt):
            continue
        score = 0
        if "." in tt:
            score += 2
        if tt in KNOWN_SIGLAS:
            score += 2
        candidates.append((score, i, tt))

    if candidates:
        candidates.sort(key=lambda x: (x[0], x[1]))
        best = candidates[-1][2]
        if best.strip(".") == "ag":
            cuerpo = "a.g."
        else:
            cuerpo = best
    return {"n_legislatura": n_leg, "fecha": fecha, "cuerpo": cuerpo}

# ----------------------------
# Encabezado global
# ----------------------------
def sanitize_global_header_text(pre: str, max_words: int = 24) -> str:
    pre = strip_control_chars(pre)
    pre = pre.replace("\r", "")
    if re.search(r"\bORDEN\s+DEL\s+D[ÍI]A\b", pre, flags=re.IGNORECASE):
        return "ORDEN DEL DÍA"
    for raw in pre.splitlines():
        ln = raw.strip()
        if not ln or likely_index_dots(ln):
            continue
        if is_good_header_line(ln):
            return normalize_header_text(ln, max_words=max_words)
    return ""

# ----------------------------
# Partir por locutores
# ----------------------------
def split_encabezado_global(text: str) -> Tuple[str, str]:
    m = SPEAKER_HEADER_RE.search(text)
    if not m:
        return text.strip(), ""
    return text[:m.start()].strip(), text[m.start():].lstrip()

def iter_turns_with_pos(text_after_header: str) -> List[Dict]:
    matches = list(SPEAKER_HEADER_RE.finditer(text_after_header))
    turns = []
    for i, m in enumerate(matches):
        start_body = m.end()
        end = matches[i+1].start() if i+1 < len(matches) else len(text_after_header)
        speaker_header = m.group("header").strip()
        body = text_after_header[start_body:end].strip()
        turns.append({"header": speaker_header, "body": body, "start": m.start(), "end": end})
    return turns

# ----------------------------
# Parser por archivo
# ----------------------------
def parse_file(path: Path, meta_idx: Dict[str, Dict]) -> List[Dict]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    raw = strip_control_chars(raw).replace("\r", "")

    encabezado_pre, resto = split_encabezado_global(raw)
    encabezado_global = sanitize_global_header_text(encabezado_pre, max_words=24)
    full = resto or raw

    headers = scan_headers_joining(full)
    turns = iter_turns_with_pos(full)

    stem = path.stem
    rec_meta = meta_idx.get(stem, {})
    infer = infer_meta_from_filename(stem)

    meta = {
        "file_name": path.name,
        "n_legislatura": rec_meta.get("n_legislatura", infer["n_legislatura"]),
        "cuerpo": rec_meta.get("cuerpo", infer["cuerpo"]),
        "fecha": rec_meta.get("fecha", infer["fecha"]),
    }

    records: List[Dict] = []

    if not turns:
        records.append({
            **meta,
            "locutor": "DESCONOCIDO",
            "encabezado": closest_prev_header(0, headers, fallback=encabezado_global),
            "intervencion": clean_intervention_text(full),
        })
        return records

    for t in turns:
        locutor = parse_locutor(t["header"])
        enc = closest_prev_header(t["start"], headers, fallback=encabezado_global)
        records.append({
            **meta,
            "locutor": locutor,
            "encabezado": enc,
            "intervencion": clean_intervention_text(t["body"]),
        })

    return records

# ----------------------------
# Índice opcional
# ----------------------------
def load_diarios_index(path="data/raw/diarios.jsonl") -> Dict[str, Dict]:
    p = Path(path)
    if not p.exists():
        return {}
    idx: Dict[str, Dict] = {}
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            key = rec.get("file_name") or rec.get("session_id") or rec.get("stem") \
                  or f"{rec.get('cuerpo','')}-{rec.get('fecha','')}"
            idx[Path(key).stem] = rec
    return idx

# ----------------------------
# Loop principal
# ----------------------------
def process_all(
    txt_dir="data/interim/txt_refined",
    out_dir="data/interim/interim_jsonl",
    diarios_index_path="data/raw/diarios.jsonl",
    consolidate_path="data/interim/debates.jsonl"
):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    meta_idx = load_diarios_index(diarios_index_path)

    txt_paths = sorted(Path(txt_dir).rglob("*.txt"))[-2000:]
    logging.info(f"Procesando {len(txt_paths)} archivos (hasta 2000) desde {txt_dir}")

    out_consolidated = Path(consolidate_path)
    if out_consolidated.exists():
        out_consolidated.unlink()

    out_consolidated_map = {
        "crr": out_consolidated.parent / "debates_crr.jsonl",
        "css": out_consolidated.parent / "debates_css.jsonl",
        "a.g.": out_consolidated.parent / "debates_ag.jsonl",
        "c.p.": out_consolidated.parent / "debates_cp.jsonl",
    }
    for path in out_consolidated_map.values():
        if path.exists():
            path.unlink()

    for p in txt_paths:
        try:
            recs = parse_file(p, meta_idx)

            out_path = Path(out_dir) / f"{p.stem}.jsonl"
            with out_path.open("w", encoding="utf-8") as f:
                for rec in recs:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")

            with out_consolidated.open("a", encoding="utf-8") as f_all:
                for rec in recs:
                    f_all.write(json.dumps(rec, ensure_ascii=False) + "\n")

            cuerpo = str(recs[0].get("cuerpo", "")).lower() if recs else ""
            name_low = p.name.lower()

            if "crr" in name_low or cuerpo == "crr":
                key = "crr"
            elif "css" in name_low or cuerpo == "css":
                key = "css"
            elif "a.g." in name_low or cuerpo in {"ag", "a.g."}:
                key = "a.g."
            elif "c.p." in name_low or cuerpo in {"cp", "c.p."}:
                key = "c.p."
            else:
                key = None

            if key and key in out_consolidated_map:
                path_target = out_consolidated_map[key]
                with path_target.open("a", encoding="utf-8") as f_cam:
                    for rec in recs:
                        f_cam.write(json.dumps(rec, ensure_ascii=False) + "\n")

            logging.info(f"{p.name}: {len(recs)} intervenciones procesadas")

        except Exception as e:
            logging.exception(f"Error en {p.name}: {e}")

    logging.info(f"\n✅ Consolidado general: {out_consolidated}")
    for k, path in out_consolidated_map.items():
        logging.info(f"✅ Consolidado {k.upper()}: {path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    process_all()
