# src/refining/clean_texts.py
# Lee data/interim/texts/*.txt y escribe data/interim/txt_refined/*.txt con limpieza/refinado.

import argparse
import json
import re
import sys
from pathlib import Path
from typing import List

# Cabeceras/pies típicos que queremos suprimir si quedaron pegados
HEADER_TOKENS = [
    "REPUBLICA ORIENTAL DEL URUGUAY","REPÚBLICA ORIENTAL DEL URUGUAY",
    "DIARIO DE SESIONES","COMISION PERMANENTE","COMISIÓN PERMANENTE",
    "CÁMARA DE REPRESENTANTES","CAMARA DE REPRESENTANTES",
    "CÁMARA DE SENADORES","CAMARA DE SENADORES",
    "ASAMBLEA GENERAL",
    "XLII LEGISLATURA","XLIII LEGISLATURA","XLIV LEGISLATURA",
    "XLV LEGISLATURA","XLVI LEGISLATURA","XLVII LEGISLATURA",
    "XLVIII LEGISLATURA","XLIX LEGISLATURA","L LEGISLATURA",
]

# Encabezado de punto tipo "4.- Título..."
TOPIC_RE = re.compile(r"^\s*\d+\s*[.\-–—]\s+.+$", re.MULTILINE)

# Líneas tipo índice con puntos guía: "Asistencias y ausencias............. 4"
DOTLEADER_RE = re.compile(r"^.+\.{3,}\s*\d+\s*$", re.MULTILINE)

# Bloques de nómina y meta previos al debate
ROSTER_LABEL_RE = re.compile(
    r"(?im)^\s*(Asisten los señores Representantes:|Con licencia:|Faltan con aviso:|Sin aviso:|Act[úu]a en el Senado:)"
)

TOPIC_HEAD_RE = re.compile(r"(?im)^\s*\d+\s*[.\-–—]\s+\S")

# Indicadores escénicos en paréntesis
STAGE_PAREN_RE = re.compile(
    r"\s*\((?=[^)]*(?:Aplausos|Risas|Murmullos|Se\s+vota|AFIRMATIVA|NEGATIVA|Unanimidad))[^)]*\)",
    re.IGNORECASE
)


# (opcional) “(Es la hora 15 y 17)”
TIME_PAREN_RE = re.compile(r"\s*\(Es la hora[^)]+\)", re.IGNORECASE)

END_SESSION_RE = re.compile(r"(?im)^\s*(?:—{2,}\s*)?Se\s+levanta\s+la\s+sesión\.\s*$")

#  Patrones para identificar la enumeración de las leyes
LEGAL_TITLE_RE = re.compile(r"(?im)^(?:\".*\"\s*)?PROYECTO\s+DE\s+LEY\b")  # p.ej. título entre comillas + PROYECTO DE LEY
ART_UNICO_RE   = re.compile(r"(?im)\bART[ÍI]CULO\s+ÚNICO\b")
ART_LINE_RE    = re.compile(r"(?im)^\s*['\"“”]?\s*ART[ÍI]CULO\s+\d+[º°]?[.\-]?\s")  # 'ARTÍCULO 30.- ...
MOTIVOS_RE     = re.compile(r"(?im)^\s*EXPOSICI[ÓO]N\s+DE\s+MOTIVOS\b")
SEDE_FECHA_RE  = re.compile(r"(?im)^\s*(Montevideo|Mdeo\.)\s*,\s*\d{1,2}\s+de\s+\w+\s+de\s+\d{4}\s*\.")
FIRMAS_LINE_RE = re.compile(r"(?m)^[A-ZÁÉÍÓÚÜÑ.'()\\-]{3,}(?:\s+[A-ZÁÉÍÓÚÜÑ.'()\\-]{2,})*(?:,\s*[A-ZÁÉÍÓÚÜÑ].*)+$")

# Limites "duros" para cerrar un bloque normativo: nuevo orador o encabezado de tema
NEXT_LIMIT_RE = re.compile(
    r"(?m)^(?:"
    r"(?:SEÑOR(?:A)?(?:\s+PRESIDENTE|A\s+PRESIDENTA)?(?:\s+[A-ZÁÉÍÓÚÜÑ.'() -]+)?)\."  # orador
    r"|"
    r"\s*\d+\s*[.\-–—]\s+\S"  # encabezado  N.- Título
    r")"
)

# Líneas administrativas sueltas
ADMIN_BULLET_RE = re.compile(
    r"""
    (?im)^\s*(?:[-–—]\s*)?                 # bullet opcional
    (?:
        A\s+(?:los|las|sus)\s+antecedentes\b.*   # "A sus/los/las antecedentes" + lo que siga
      | A\s+la\s+Comisi[oó]n\b.*                 # "A la Comisión ..." + lo que siga
      | Se\s+[a-záéíóúüñ]+                       # "Se repartieron / Se cursó / Se remitieron ..."
        (?:\s+[a-záéíóúüñ]+){0,6}
        (?:\s+con\s+fecha\b[^\n]*)?
    )\s*$
    """,
    re.VERBOSE
)


LOWER_RE = re.compile(r"[a-záéíóúüñ]")

# Oradores
SPEAKER_RE = re.compile(
    r"""^
    (?:SEÑOR(?:A)?(?:\s+PRESIDENTE|A\s+PRESIDENTA)?      # SEÑOR/SEÑORA (PRESIDENTE|PRESIDENTA) opcional
     (?:\s+[A-ZÁÉÍÓÚÜÑ.'-]+(?:\s+[A-ZÁÉÍÓÚÜÑ.'()-]+){0,6})?  # nombre/apellidos opcionales (incluye paréntesis)
    )
    \.\s*[-–—]?\s*$                                        # punto y rayita final
    """,
    flags=re.MULTILINE | re.VERBOSE
)

# Procedurales sueltos (líneas completas)
PROCEDURAL_LINE_RE = re.compile(
    r"""(?im)^\s*(
        Léase|Se\s+lee|En\s+discusión(?:\s+(?:general|particular))?|
        En\s+consideración|Se\s+va\s+a\s+votar|Se\s+vota|Se\s+entra\s+al\s+orden\s+del\s+d[íi]a\b|
        Intermedio\.?|Continúa\s+la\s+sesión\.?|La\s+Cámara\s+pasa\s+a\s+intermedio\.?|
        La\s+Mesa\s+entiende|Queda\s+convocado|RESUELVE:|ANTECEDENTES:
    )\b.*$""",
    re.VERBOSE
)

SPEAKER_INLINE_RE = re.compile(
    r"""(?mx)^
        \s*
        (?:\((?:Di[aá]logos|Risas|Murmullos)[^)]*\)\s*)?
        (?:SEÑOR(?:A)?
           (?:\s+PRESIDENTE|\s+PRESIDENTA)?      # PRESIDENTE/A opcional
           (?:\s*\([^)]+\))?                     # (Apellido) opcional
           (?:\s+[A-ZÁÉÍÓÚÜÑ.'()-]+){0,6}        # apellidos opcionales
        )
        \s*(?:\.-|:)\s
    """
)

# Límite por defecto (lo que estás usando hoy): nuevo orador (formato clásico) o encabezado numerado
NEXT_LIMIT_RE_DEFAULT = re.compile(
    r"(?m)^(?:"
    r"(?:SEÑOR(?:A)?(?:\s+PRESIDENTE|A\s+PRESIDENTA)?(?:\s+[A-ZÁÉÍÓÚÜÑ.'() -]+)?)\."
    r"|"
    r"\s*\d+\s*[.\-–—]\s+\S"
    r")"
)

# Límite SOLO para CRR: acepta también orador inline y “(Diálogos) …”
NEXT_LIMIT_RE_CRR = re.compile(
    r"""(?mx)^
        (?:
          \s*
          (?:\((?:Di[aá]logos|Risas|Murmullos)[^)]*\)\s*)?
          (?:SEÑOR(?:A)?(?:\s+PRESIDENTE|\s+PRESIDENTA)?
             (?:\s*\([^)]+\))?
             (?:\s+[A-ZÁÉÍÓÚÜÑ.'()-]+){0,6}
          )
          \s*(?:\.-|:)\s
          |
          \s*\d+\s*[.\-–—]\s+\S
        )
    """
)



# Resultados de votación (con rayas y conteos)
VOTE_RESULT_RE = re.compile(r"(?im)^\s*—{2,}\s*\d+\s+en\s+\d+:\s*(AFIRMATIVA|NEGATIVA)\.?\s*$")

# Eventos procedimentales con rayas incrustados en línea
INLINE_DASH_EVENT_RE = re.compile(r"\s*—{2,}[^ \n].*")

SKIP_OD_TITLES_RE = re.compile(r"(?i)\b(?:Asuntos\s+entrados|Proyectos\s+presentados)\b")

VOTE_RESULT_FLEX_RE = re.compile(r"(?im)^\s*—{2,}.*\b(AFIRMATIVA|NEGATIVA)\b.*$")

ACTA_HINT_RE = re.compile(r"(?im)\b(?:Se\s+lee|Se\s+vota|Dese\s+cuenta|En\s+discusi[oó]n)\b")
OD_HEAD_RE   = re.compile(r"(?m)^\s*\d+\s*[.\-–—]\s+\S")

WEIRD_CHARS_RE = re.compile(r"[~^´`¨·¬<>|{}]+")
YEAR_RE = re.compile(r"\b(1[789]\d{2}|20\d{2})\b")  # 1700-2099
ORDEN_DIA_RE = re.compile(r"\bORDEN\s+DEL\s+D[ÍI]A\b", re.IGNORECASE)

def _ocr_noise_ratio(s: str) -> float:
    if not s:
        return 0.0
    weird = len(WEIRD_CHARS_RE.findall(s))
    return weird / max(1, len(s))

def _looks_like_bibliography_or_ocr(raw: str) -> bool:
    # muchos años + caracteres raros + sin señales fuertes de acta
    years = len(YEAR_RE.findall(raw))
    noise = _ocr_noise_ratio(raw)
    has_topics = bool(TOPIC_RE.search(raw)) or bool(ORDEN_DIA_RE.search(raw))
    has_speakers = bool(SPEAKER_RE.search(raw))
    # umbrales conservadores
    return (years >= 10 or noise >= 0.002) and not (has_topics or has_speakers)



def _strip_procedural_lines(text: str) -> str:  ### Revisar si quitamos o no resultados de conteos.
    # Líneas procedimentales tipo "Léase", "Se lee", "En discusión", etc.
    text = PROCEDURAL_LINE_RE.sub("", text)

    # Quitar restos en paréntesis frecuentes
    text = re.sub(r"\s*\(Se\s+vota\)\s*", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\(Se\s+lee:?[\s)]*", " ", text, flags=re.IGNORECASE)

    # Resultados de votación con rayas y conteos
    text = VOTE_RESULT_RE.sub("", text)

    text = VOTE_RESULT_FLEX_RE.sub("", text)

    # Compactar
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text

def _postfix_touches(text: str) -> str:
    # Quita eventos procedimentales con rayas si quedaron incrustados en una línea
    text = INLINE_DASH_EVENT_RE.sub("", text)
    # Fuerza salto antes de etiquetas de orador en MAYÚSCULAS si no empiezan la línea
    text = re.sub(
        r"(?<!\n)(?=SEÑOR(?:A)?(?:\s+PRESIDENTE|A\s+PRESIDENTA)?\b)",
        "\n",
        text
    )
    # Compacta saltos
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text

def _extract_normative_blocks_anywhere(
    text: str,
    min_chars: int = 500,
    next_limit_re: re.Pattern | None = None,
    file_stem: str = ""
) -> tuple[str, list]:
    """
    Extrae bloques normativos (proyecto, artículos, exposición de motivos, firmas)
    en cualquier parte del texto. Devuelve (texto_sin_normativa, bloques).

    Cambios clave:
    - En archivos CRR (is_crr=True) la detección por "densidad de ARTÍCULO" requiere
      un título legal cercano (PROYECTO/ARTÍCULO ÚNICO/MOTIVOS) y aplicamos guardarraíles
      extra para evitar recortes de actas.
    - Global: si el supuesto bloque arranca pegado a un encabezado del Orden del Día,
      no se corta.
    """
    use_next_limit = next_limit_re or NEXT_LIMIT_RE_DEFAULT

    L = len(text)
    anchors = []

    is_crr = "crr" in (file_stem or "").lower()

    # --- Helper local: título legal cerca del ancla ---
    NEARBY_TITLE_WINDOW = 400  # bytes alrededor del ancla
    def _has_nearby_legal_title(pos: int) -> bool:
        start = max(0, pos - NEARBY_TITLE_WINDOW)
        end   = min(L, pos + NEARBY_TITLE_WINDOW)
        window = text[start:end]
        return bool(
            LEGAL_TITLE_RE.search(window) or
            ART_UNICO_RE.search(window) or
            MOTIVOS_RE.search(window)
        )

    # 1) Anclas fuertes (títulos claros)
    for m in LEGAL_TITLE_RE.finditer(text):
        anchors.append(("proyecto_de_ley", m.start()))
    for m in MOTIVOS_RE.finditer(text):
        anchors.append(("exposicion_de_motivos", m.start()))
    for m in ART_UNICO_RE.finditer(text):
        anchors.append(("articulo_unico", m.start()))

    # 2) Anclas por densidad de ARTÍCULO
    #    Solo en CRR exigimos título legal cercano; fuera de CRR mantenemos tu criterio previo.
    for m in ART_LINE_RE.finditer(text):
        start = m.start()
        window = text[start:start + (1800 if is_crr else 3500)]
        min_count = 3 if is_crr else 2
        if len(list(ART_LINE_RE.finditer(window))) >= min_count:
            if (not is_crr) or _has_nearby_legal_title(start):
                anchors.append(("articulos_densos", start))

    if not anchors:
        return text, []

    anchors.sort(key=lambda x: x[1])

    blocks, cut_ranges = [], []

    for idx, (kind, start) in enumerate(anchors):
        # candidatos de cierre: siguiente ancla, siguiente orador/tema, fin
        end_candidates = []
        if idx + 1 < len(anchors):
            end_candidates.append(anchors[idx + 1][1])
        m_lim = use_next_limit.search(text, pos=start + 1)
        if m_lim:
            end_candidates.append(m_lim.start())
        end_candidates.append(L)
        end = min(end_candidates)

        chunk = text[start:end]

        # señales normativas
        n_art = len(list(ART_LINE_RE.finditer(chunk)))
        has_mot = bool(MOTIVOS_RE.search(chunk))
        has_fecha = bool(SEDE_FECHA_RE.search(chunk))
        has_firmas = bool(FIRMAS_LINE_RE.search(chunk))
        long_enough = len(chunk) >= min_chars
        has_speaker_inside = bool(SPEAKER_RE.search(chunk))

        # --- Guardarraíles contextuales ---
        # No cortar si el bloque arranca pegado a un encabezado del Orden del Día (global)
        prev_break = text.rfind("\n", 0, start)
        line_start = 0 if prev_break == -1 else prev_break + 1
        line_text = text[line_start:start+1]
        starts_at_od_head = bool(OD_HEAD_RE.match(line_text))

        # “Huele a acta/trámite” (solo CRR): Se lee / Se vota / Dese cuenta / En discusión
        has_acta_hints = bool(ACTA_HINT_RE.search(chunk)) if is_crr else False

        # --- Aceptación del bloque ---
        is_titleish = bool(
            LEGAL_TITLE_RE.search(chunk[:200]) or
            ART_UNICO_RE.search(chunk[:200]) or
            MOTIVOS_RE.search(chunk[:200])
        )
        # Solo en CRR pedimos marco admin fuerte cuando usamos (fecha+firmas)
        strict_admin_needed = is_crr

        accept = (
            has_mot
            or (has_fecha and has_firmas and (not strict_admin_needed or re.search(
                r"(?im)\b(Sala de la Comisi[oó]n|VISTO:|RESULTANDO:|ATENTO:|RESUELVE:)\b", chunk)))
            or (long_enough and is_titleish)
            or (n_art >= (3 if is_crr else 2) and is_titleish)  # densidad sola NO alcanza
        )

        # Guardarraíles finales
        if starts_at_od_head:
            accept = False                # global
        if has_speaker_inside and (is_crr or kind == "articulos_densos"):
            accept = False
        if has_acta_hints:
            accept = False                # solo CRR

        if accept:
            blocks.append({
                "file_span": (start, end),
                "kind": kind,
                "n_articulos": n_art,
                "has_motivos": has_mot,
                "has_firmas": has_firmas,
                "text": chunk.strip()
            })
            cut_ranges.append((start, end))

    if not cut_ranges:
        return text, []

    # 3) Remover bloques del texto y limpiar saltos
    cut_ranges.sort(key=lambda x: x[0])
    out, last = [], 0
    for s, e in cut_ranges:
        out.append(text[last:s])
        last = e
    out.append(text[last:])
    cleaned = "".join(out)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip() + "\n"
    return cleaned, blocks



def _cut_from_orden_del_dia(text: str) -> str:
    # Si existe "ORDEN DEL DÍA", conservar desde el primer encabezado numerado SIGUIENTE
    # que NO sea "Asuntos entrados" ni "Proyectos presentados".
    m = re.search(r"\bORDEN\s+DEL\s+D[ÍI]A\b", text, flags=re.IGNORECASE)
    if m:
        rest = text[m.end():]
        # Iteramos por todos los encabezados numerados en "rest" hasta dar con uno válido
        for h in re.finditer(r"(?m)^\s*\d+\s*[.\-–—]\s+\S", rest):
            line_end = rest.find("\n", h.start())
            line = rest[h.start():] if line_end == -1 else rest[h.start():line_end]
            if not SKIP_OD_TITLES_RE.search(line):
                return rest[h.start():]
    # si no hay "ORDEN DEL DÍA", fallback al primer encabezado numérico global
    return _cut_before_first_topic(text)


def _normalize_unicode(s: str) -> str:
    """Deja los caracteres Unicode tal cual; no elimina ni reemplaza nada."""
    return s

def _fix_spaced_caps(line: str) -> str:
    """
    Une secuencias de MAYÚSCULAS separadas por espacios: 'C O M I S I O N' -> 'COMISION'.
    Evita tocar palabras normales (con minúsculas).
    """
    def _join(match: re.Match) -> str:
        return match.group(0).replace(" ", "")
    # >= 3 letras mayúsculas separadas por espacios
    return re.sub(r"(?:(?<=\b)|^)(?:[A-ZÁÉÍÓÚÜÑ]\s){2,}[A-ZÁÉÍÓÚÜÑ](?=\b)", _join, line)

def _dehyphenate_eol(text: str) -> str:
    # Une solo si la 2ª parte arranca en minúscula (evita "me-\nSobre" -> "meSobre")
    return re.sub(
        r"([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{2,})[\-­]\s*\n\s*([a-záéíóúüñ]{2,})",
        r"\1\2",
        text
    )

def _drop_page_artifacts(line: str) -> bool:
    t = line.strip()
    if not t:
        return False
    # 🚫 No eliminar si es un encabezado de tema válido:
    if TOPIC_RE.match(t):
        return False
    # número de página suelto
    if re.fullmatch(r"\d{1,4}", t):
        return True
    # pie tipo '102 - C.P. ...'
    if re.match(r"^\d+\s*[-–—]?\s*C\.?\s*P\.?.*$", t, flags=re.IGNORECASE):
        return True

    for tok in HEADER_TOKENS:
        # Solo considerar encabezado/pie si NO hay minúsculas (típico de cabeceras)
        if tok.lower() in t.lower() and not LOWER_RE.search(t):
            return True

    if len(t) <= 2 and not re.search(r"[A-Za-zÁÉÍÓÚÜÑ0-9]", t):
        return True
    return False


def _smart_join_lines_conservative(lines: List[str]) -> str:
    out: List[str] = []
    for i, cur in enumerate(lines):
        cur = _fix_spaced_caps(cur)
        out.append(cur.strip())
    text = "\n".join(out)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()



def _smart_join_lines(lines: List[str]) -> str:
    out: List[str] = []
    i = 0
    while i < len(lines):
        cur = lines[i]
        cur = _fix_spaced_caps(cur)

        # --- Bloques que se preservan tal cual ---
        if TOPIC_RE.match(cur.strip()):
            # inserta una línea en blanco antes del encabezado (si no es el primero)
            if out and out[-1].strip() != "":
                out.append("")
            out.append(cur.strip())
            # asegura una línea en blanco después del encabezado
            out.append("")
            i += 1
            continue

        if SPEAKER_RE.match(cur.strip()):
            if out and out[-1].strip() != "":
                out.append("")
            out.append(cur.strip())
            i += 1
            continue

        # --- Lógica de unión “suave” de párrafos normales ---
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        cur_strip = cur.rstrip()
        nxt_strip = nxt.lstrip()

        no_join = False
        if re.search(r"[.!?;:–—]\s*$", cur_strip):
            no_join = True
        if nxt_strip == "":
            no_join = True
        if re.match(r"^\s*(?:[-–•]|[\(\[]?\d{1,2}[\)\].])\s+", nxt_strip):
            no_join = True
        if nxt_strip and nxt_strip == nxt_strip.upper() and len(nxt_strip) <= 80:
            no_join = True
        if SPEAKER_RE.match(nxt_strip) or TOPIC_RE.match(nxt_strip):
            no_join = True

        if no_join:
            out.append(cur_strip)
            i += 1
        else:
            joined = (cur_strip + " " + nxt_strip).strip()
            lines[i + 1] = joined
            i += 1

    text = "\n".join(out)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def _cut_before_first_topic(text: str) -> str:
    """
    Conserva desde el primer encabezado de punto ("N.- ...") inclusive
    y descarta TODO lo anterior (SUMARIO, nóminas, etc.).
    """
    m = re.search(r"(?m)^\s*\d+\s*[.\-–—]\s+\S", text)
    if not m:
        return text
    return text[m.start():]

def _strip_summary(text: str) -> str:
    # Quitar bloque SUMARIO completo hasta el próximo "ORDEN DEL DÍA" o un encabezado numérico tipo "4.- ..."
    text = re.sub(
        r"(?is)\bSUMARIO\b.*?(?=\bORDEN\s+DEL\s+D[ÍI]A\b|\n\s*\d+\s*[.\-–—]\s+)",
        "",
        text,
    )
    # Quitar líneas de índice con puntos guía
    text = DOTLEADER_RE.sub("", text)
    return text

def _normalize_orden_del_dia(text: str) -> str:
    # Caso "ORDEN DEL DÍA 4.- ..." -> dejar salto antes del "4.- ..."
    text = re.sub(r"\bORDEN\s+DEL\s+D[ÍI]A\b\s*(\d+\s*[.\-–—]\s+)", r"ORDEN DEL DÍA\n\1", text, flags=re.IGNORECASE)

    # Caso "- ORDEN DEL DÍA - Título ..." -> conservar ancla normalizada
    text = re.sub(r"(?im)^\s*-\s*ORDEN\s+DEL\s+D[ÍI]A\s*-\s*.*$", "ORDEN DEL DÍA", text)

    # Compactar
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text

def _strip_roster_and_stage(text: str) -> str:
    lines = text.splitlines()
    out = []
    skipping_roster = False

    for ln in lines:
        if ROSTER_LABEL_RE.match(ln):
            skipping_roster = True
            continue

        if skipping_roster:
            # seguimos ignorando hasta línea en blanco o hasta un encabezado numerado
            if not ln.strip() or TOPIC_HEAD_RE.match(ln):
                skipping_roster = False
            else:
                continue  # aún dentro de nómina
        # líneas administrativas sueltas
        if ADMIN_BULLET_RE.match(ln):
            continue

        out.append(ln)

    text = "\n".join(out)

    # Paréntesis escénicos (ver regex más flexible abajo)
    text = STAGE_PAREN_RE.sub("", text)

    # Rayas “—— …” (líneas completas)
    text = re.sub(r"(?m)^\s*—{2,}.*$", "", text)

    # “(Es la hora …)”
    text = TIME_PAREN_RE.sub("", text)

    # Cierre
    text = END_SESSION_RE.sub("", text)

    # Compactar saltos
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


# --- DEBUG/PROBES: medir caídas paso a paso dentro de clean_texts.py ---

def refine_text_with_probes(
    raw: str,
    *,
    min_chars: int = 500,
    save_dir: Path | None = None,
    tag: str = "",
    next_limit_re: re.Pattern | None = None
) -> tuple[str, dict]:
    """
    Igual que refine_text, pero devuelve (texto_final, métricas_por_paso).
    Si save_dir no es None, guarda snapshots de texto intermedio con prefijo tag.
    """

    # 🚦 Rama light también en debug (igual que refine_text)
    if _looks_like_bibliography_or_ocr(raw):
        s = _refine_text_light(raw)
        stats = {
            "n_legal_blocks": 0,
            # métricas mínimas/consistentes para no romper compare_runs
            "len_01_norm_unicode": len(raw),
            "len_02_dehyphen": len(raw),
            "len_03_strip_summary": len(s),
            "len_04_smart_join": len(s),
            "len_05_cut_from_od": len(s),
            "len_06_norm_orden_dia": len(s),
            "len_07_strip_roster_stage": len(s),
            "len_08_strip_procedural": len(s),
            "len_09_postfix": len(s),
            "len_10_after_legal_cut": len(s),
            "len_11_final": len(s),
            "ratio_after_summary": 1.0,
            "ratio_after_procedural": 1.0,
            "ratio_after_legal": 1.0,
        }
        return s, stats

    stats = {}
    out_snap = []

    def snap(name: str, s: str):
        stats[f"len_{name}"] = len(s)
        if save_dir:
            save_dir.mkdir(parents=True, exist_ok=True)
            (save_dir / f"{tag}__{name}.txt").write_text(s, encoding="utf-8")
        out_snap.append((name, len(s)))

    s = _normalize_unicode(raw);                      snap("01_norm_unicode", s)
    s = _dehyphenate_eol(s);                          snap("02_dehyphen", s)

    s = _strip_summary(s);                            snap("03_strip_summary", s)

    lines = []
    for ln in s.splitlines():
        if _drop_page_artifacts(ln):
            continue
        ln = re.sub(r"[ \t]+", " ", ln)
        if len(ln.strip()) == 1 and re.match(r"[A-Za-zÁÉÍÓÚÜÑ]", ln.strip()):
            continue
        lines.append(ln.rstrip())
    s2 = _smart_join_lines(lines);                    snap("04_smart_join", s2)

    s2 = _cut_from_orden_del_dia(s2);                 snap("05_cut_from_od", s2)

    s2 = _normalize_orden_del_dia(s2);                snap("06_norm_orden_dia", s2)

    s2 = _strip_roster_and_stage(s2);                 snap("07_strip_roster_stage", s2)

    s2 = _strip_procedural_lines(s2);                 snap("08_strip_procedural", s2)

    s2 = _postfix_touches(s2);                        snap("09_postfix", s2)

    s2, legal_blocks = _extract_normative_blocks_anywhere(
        s2, min_chars=min_chars, next_limit_re=next_limit_re, file_stem=tag
    )
    snap("10_after_legal_cut", s2)
    stats["n_legal_blocks"] = len(legal_blocks)

    s2 = re.sub(r"\s+([,.;:!?])", r"\1", s2)
    s2 = re.sub(r"[ \t]{2,}", " ", s2)
    s2 = s2.strip() + "\n";                           snap("11_final", s2)

    # ratios útiles
    stats["ratio_after_summary"]   = stats["len_03_strip_summary"] / max(1, stats["len_01_norm_unicode"])
    stats["ratio_after_procedural"] = stats["len_08_strip_procedural"] / max(1, stats["len_07_strip_roster_stage"])
    stats["ratio_after_legal"]     = stats["len_10_after_legal_cut"] / max(1, stats["len_09_postfix"])

    return s2, stats


def _refine_text_light(raw: str) -> str:
    s = _normalize_unicode(raw)
    s = _dehyphenate_eol(s)

    # solo limpieza MUY básica de SUMARIO/índices si aparecen
    s = _strip_summary(s)

    # filtrar artefactos de página, pero sin tocar encabezados (ya contemplado)
    lines = []
    for ln in s.splitlines():
        if _drop_page_artifacts(ln):
            continue
        ln = re.sub(r"[ \t]+", " ", ln)
        lines.append(ln.rstrip())

    # join conservador
    s2 = _smart_join_lines_conservative(lines)

    # no cortar por ORDEN DEL DÍA, no quitar nóminas, no recorte normativo
    # solo limpieza mínima de procedimentales y rayas sueltas (prudente)
    s2 = re.sub(r"\s*\(Se\s+lee:?[\s)]*", " ", s2, flags=re.IGNORECASE)
    s2 = re.sub(r"\s*\(Se\s+vota\)\s*", " ", s2, flags=re.IGNORECASE)
    s2 = re.sub(r"(?m)^\s*—{2,}.*$", "", s2)

    # espacios/puntuación
    s2 = re.sub(r"\s+([,.;:!?])", r"\1", s2)
    s2 = re.sub(r"[ \t]{2,}", " ", s2)

    return s2.strip() + "\n"


def refine_text(raw: str, *, next_limit_re: re.Pattern | None = None) -> str:
    # Si parece bibliografía/ruido OCR y no acta, usar pipeline liviano
    if _looks_like_bibliography_or_ocr(raw):
        return _refine_text_light(raw)

    s = _normalize_unicode(raw)
    s = _dehyphenate_eol(s)

    # Primero sacar SUMARIO en el texto bruto
    s = _strip_summary(s)

    lines = []
    for ln in s.splitlines():
        if _drop_page_artifacts(ln):
            continue
        ln = re.sub(r"[ \t]+", " ", ln)
        if len(ln.strip()) == 1 and re.match(r"[A-Za-zÁÉÍÓÚÜÑ]", ln.strip()):
            continue
        lines.append(ln.rstrip())

    s2 = _smart_join_lines(lines)

    # Recortar primero usando el ancla
    s2 = _cut_from_orden_del_dia(s2)

    # Recién ahora normalizamos cómo se ve el rótulo
    s2 = _normalize_orden_del_dia(s2)

    # Limpiar nóminas/escena sin tocar encabezados
    s2 = _strip_roster_and_stage(s2)

    # Procedurales y votos (líneas sueltas)
    s2 = _strip_procedural_lines(s2)

    # Toques finales: rayas incrustadas y salto antes de oradores
    s2 = _postfix_touches(s2)

    # Corta normativa (si corresponde)
    s2, _legal_blocks = _extract_normative_blocks_anywhere(
        s2, min_chars=500, next_limit_re=next_limit_re
    )

    # Puntuación/espacios finales
    s2 = re.sub(r"\s+([,.;:!?])", r"\1", s2)
    s2 = re.sub(r"[ \t]{2,}", " ", s2)
    return s2.strip() + "\n"


def _is_crr_filename(path: Path) -> bool:
    nm = path.name.lower()
    # alcanza con que contenga 'crr' (tus nombres son tipo 47_2013-11-13_crr.txt)
    return "crr" in nm

    


def process_file(src: Path, dst: Path, overwrite: bool=False) -> bool:
    if dst.exists() and not overwrite:
        return True
    try:
        raw = src.read_text(encoding="utf-8", errors="ignore")
        cleaned = refine_text(raw)
        dst.write_text(cleaned, encoding="utf-8")
        return True
    except Exception as e:
        print(f"[FAIL] {src.name}: {e}", file=sys.stderr)
        return False

def main():
    import csv, datetime
    ap = argparse.ArgumentParser(
        description="Refina/limpia .txt de data/interim/texts y escribe en data/interim/txt_refined (con subcarpetas por cámara)."
    )
    ap.add_argument("--src", default="data/interim/texts", help="Directorio origen (default: data/interim/texts)")
    ap.add_argument("--out", default="data/interim/txt_refined", help="Directorio salida base (default: data/interim/txt_refined)")
    ap.add_argument("--overwrite", action="store_true", help="Sobrescribir archivos existentes")
    ap.add_argument("--glob", default="*.txt", help="Patrón de archivos (default: *.txt)")
    ap.add_argument("--limit", type=int, default=0, help="Limitar a N archivos (0 = sin límite)")
    ap.add_argument("--debug-csv", default="", help="Ruta CSV para métricas por paso (opcional)")
    ap.add_argument("--debug-snapshots", default="", help="Dir para guardar textos intermedios (opcional)")
    ap.add_argument("--min-chars", type=int, default=500, help="Umbral min de longitud para normativa (default: 500)")
    args = ap.parse_args()

    src_dir = Path(args.src)
    out_base = Path(args.out)
    out_base.mkdir(parents=True, exist_ok=True)

    txts = sorted(src_dir.glob(args.glob))
    if args.limit and args.limit > 0:
        txts = txts[:args.limit]
    if not txts:
        print(f"No encontré TXT en {src_dir.resolve()}", file=sys.stderr)
        sys.exit(1)

    # === CSV de debug opcional ===
    writer = None
    if args.debug_csv:
        debug_csv_path = Path(args.debug_csv)
        write_header = not debug_csv_path.exists()
        debug_csv_path.parent.mkdir(parents=True, exist_ok=True)
        fcsv = debug_csv_path.open("a", newline="", encoding="utf-8")
        cols = ["file_name", "n_legal_blocks"] + [
            "len_01_norm_unicode", "len_02_dehyphen", "len_03_strip_summary",
            "len_04_smart_join", "len_05_cut_from_od", "len_06_norm_orden_dia",
            "len_07_strip_roster_stage", "len_08_strip_procedural", "len_09_postfix",
            "len_10_after_legal_cut", "len_11_final",
            "ratio_after_summary", "ratio_after_procedural", "ratio_after_legal"
        ]
        writer = csv.DictWriter(fcsv, fieldnames=cols)
        if write_header:
            writer.writeheader()
    else:
        fcsv = None

    ok = bad = 0
    try:
        for t in txts:
            try:
                raw = t.read_text(encoding="utf-8", errors="ignore")

                # 🔹 Detectar cámara por nombre de archivo
                name_low = t.name.lower()
                if "css" in name_low:
                    chamber = "css"
                elif "crr" in name_low:
                    chamber = "crr"
                elif "ag" in name_low or "a.g." in name_low:
                    chamber = "a.g."
                elif "cp" in name_low or "c.p." in name_low:
                    chamber = "c.p."
                else:
                    chamber = "otros"

                # 🔹 Crear subcarpeta dentro de out_base
                chamber_dir = out_base / chamber
                chamber_dir.mkdir(parents=True, exist_ok=True)

                dst = chamber_dir / t.name

                next_limit = NEXT_LIMIT_RE_CRR if "crr" in name_low else NEXT_LIMIT_RE_DEFAULT

                if args.debug_csv:
                    snap_dir = Path(args.debug_snapshots) if args.debug_snapshots else None
                    cleaned, stats = refine_text_with_probes(
                        raw,
                        min_chars=args.min_chars,
                        save_dir=snap_dir,
                        tag=t.stem,
                        next_limit_re=next_limit
                    )
                    dst.write_text(cleaned, encoding="utf-8")

                    row = {"file_name": t.name, "n_legal_blocks": stats.get("n_legal_blocks", 0)}
                    for k in ("len_01_norm_unicode","len_02_dehyphen","len_03_strip_summary",
                              "len_04_smart_join","len_05_cut_from_od","len_06_norm_orden_dia",
                              "len_07_strip_roster_stage","len_08_strip_procedural","len_09_postfix",
                              "len_10_after_legal_cut","len_11_final",
                              "ratio_after_summary","ratio_after_procedural","ratio_after_legal"):
                        row[k] = stats.get(k, 0)
                    writer.writerow(row)
                    print(f"[OK] {t.name} ({chamber}) (debug) -> {dst.name}")
                    ok += 1
                else:
                    cleaned = refine_text(raw, next_limit_re=next_limit)
                    dst.write_text(cleaned, encoding="utf-8")
                    print(f"[OK] {t.name} ({chamber}) -> {dst.name}")
                    ok += 1

            except Exception as e:
                print(f"[FAIL] {t.name}: {e}", file=sys.stderr)
                bad += 1
    finally:
        if fcsv:
            fcsv.close()

    print(f"\nListo. OK={ok} FAIL={bad}")
    print(f"Salida base: {out_base.resolve()}")


    print(f"\nListo. OK={ok} FAIL={bad}")
    print(f"Salida base: {out_base.resolve()}")




if __name__ == "__main__":
    main()