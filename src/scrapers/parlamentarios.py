# -------------------------------------------------------
# Scraper Playwright: Asamblea General (Actuantes)
# - Recorre desde 2010-02-15 hasta hoy (UTC-3 Montevideo)
# - Guarda CSV incremental en data/raw/parlamentarios/ag_actuantes_2010_hoy.csv
# - Genera CSV adicional agrupado por legislatura (sin duplicados)
# - Usa checkpoint en data/raw/parlamentarios/checkpoints/asamblea_general.last
# - Log en data/logs/parlamentarios_scraper.log
# -------------------------------------------------------

import asyncio
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Iterable
import re
import json
import pandas as pd
from dateutil.tz import gettz
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

# ===================== RUTAS/ARCHIVOS =====================
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "parlamentarios"
CKPT_DIR = DATA_DIR / "checkpoints"
LOGS_DIR = PROJECT_ROOT / "data" / "logs"

for d in (DATA_DIR, CKPT_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = DATA_DIR / "ag_actuantes_2010_hoy.csv"
OUTPUT_CSV_LEG = DATA_DIR / "ag_actuantes_legislaturas.csv"
CHECKPOINT = CKPT_DIR / "asamblea_general.last"
RUN_LOG = LOGS_DIR / "parlamentarios_scraper.log"

# ===================== LOGGING ============================
logging.basicConfig(
    filename=str(RUN_LOG),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("parlamentarios")
log.addHandler(logging.StreamHandler(sys.stdout))

# ===================== CONFIG SCRAPER =====================
BASE_URL = "https://parlamento.gub.uy/sobreelparlamento/integracionhistorica"
TZ = gettz("America/Montevideo")
START_DATE = datetime(2010, 2, 15, tzinfo=TZ).date()
END_DATE = datetime.now(TZ).date()

NAV_TIMEOUT = 25_000
ACTION_TIMEOUT = 15_000
RETRIES = 3
COOLDOWN_DAY_MS = 900

# horas a probar por cada fecha
HOUR_CANDIDATES = ["12:00", "23:59", "10:00", "16:00"]

# fechas candidatas para no scrapear todos los días
DATES_JSONL = PROJECT_ROOT / "data" / "raw" / "diarios.jsonl"
DATES_CSV   = PROJECT_ROOT / "data" / "asistencias_ag.csv"
USE_DATES_ONLY = True

# ===================== LEGISLATURAS =====================
LEGISLATURES = [
    {"value": "50", "romano": "L",      "desde": "2025-02-15", "hasta": "2030-02-14"},
    {"value": "49", "romano": "XLIX",   "desde": "2020-02-15", "hasta": "2025-02-14"},
    {"value": "48", "romano": "XLVIII", "desde": "2015-02-15", "hasta": "2020-02-14"},
    {"value": "47", "romano": "XLVII",  "desde": "2010-02-15", "hasta": "2015-02-14"},
]
for leg in LEGISLATURES:
    leg["desde_date"] = datetime.strptime(leg["desde"], "%Y-%m-%d").date()
    leg["hasta_date"] = datetime.strptime(leg["hasta"], "%Y-%m-%d").date()

def fecha_a_legislatura(fecha_str: str) -> Optional[str]:
    try:
        f = datetime.strptime(fecha_str, "%Y-%m-%d").date()
    except Exception:
        return None
    for leg in LEGISLATURES:
        if leg["desde_date"] <= f <= leg["hasta_date"]:
            return leg["romano"]
    return None

# ===================== UTILIDADES DF =====================
def load_existing() -> pd.DataFrame:
    if OUTPUT_CSV.exists():
        try:
            return pd.read_csv(OUTPUT_CSV)
        except Exception as e:
            log.warning(f"No pude leer {OUTPUT_CSV.name}: {e}")
    return pd.DataFrame(columns=[
        "fecha","nombre","partido","camara","condicion","sustituye_a","detalles_texto","fuente_url"
    ])

def build_legislatura_view(df_full: pd.DataFrame) -> pd.DataFrame:
    if df_full.empty:
        return pd.DataFrame(columns=[
            "legislatura","nombre","partido","camara","condicion","sustituye_a","detalles_texto"
        ])
    tmp = df_full.copy()
    tmp["legislatura"] = tmp["fecha"].apply(fecha_a_legislatura)
    tmp = tmp[~tmp["legislatura"].isna()].copy()
    cols = ["legislatura","nombre","partido","camara","condicion","sustituye_a","detalles_texto"]
    for c in cols:
        if c not in tmp.columns:
            tmp[c] = ""
    tmp = tmp[cols]
    tmp = tmp.drop_duplicates().reset_index(drop=True)
    return tmp

def read_checkpoint() -> Optional[date]:
    if CHECKPOINT.exists():
        try:
            return datetime.strptime(CHECKPOINT.read_text().strip(), "%Y-%m-%d").date()
        except Exception as e:
            log.warning(f"Checkpoint corrupto: {e}")
    return None

def write_checkpoint(d: date):
    CHECKPOINT.write_text(d.strftime("%Y-%m-%d"))

# ===================== CARGA FECHAS CANDIDATAS =====================
def _parse_date(s: str):
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def load_dates_from_jsonl(path: Path) -> list[dict]:
    """
    Devuelve lista de dicts: [{"fecha": date, "cuerpo": "CSS"}, ...]
    """
    resultados = []
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                continue

            cuerpo_raw = (obj.get("cuerpo") or "").strip().upper()
            fecha_raw = obj.get("fecha") or obj.get("date") or obj.get("fecha_sesion")

            if not fecha_raw:
                continue

            d = _parse_date(str(fecha_raw))
            if not d:
                continue

            # mapeo del JSON -> valor de formulario
            if cuerpo_raw == "A.G.":
                cuerpo_form = "Asamblea General"
            elif cuerpo_raw == "CRR":
                cuerpo_form = "Cámara de Representantes"
            elif cuerpo_raw == "CSS":
                cuerpo_form = "Cámara de Senadores"
            else:
                continue

            resultados.append({
                "fecha": d,
                "cuerpo": cuerpo_form
            })

    return sorted(resultados, key=lambda x: x["fecha"])

def load_dates_from_csv(path: Path) -> list[date]:
    dates = set()
    if not path.exists():
        return []
    dfc = pd.read_csv(path, dtype=str)
    cuerpo_cols = [c for c in dfc.columns if c.lower() in ["cuerpo","camara","cámara"]]
    if cuerpo_cols:
        col = cuerpo_cols[0]
        dfc = dfc[dfc[col].str.contains("asamblea", case=False, na=False)]
    fcol = [c for c in dfc.columns if c.lower() in ["fecha","date","fecha_sesion","fecha_sesión"]]
    if not fcol:
        fcol = [dfc.columns[0]]
    for v in dfc[fcol[0]].dropna().astype(str):
        dd = _parse_date(v)
        if dd:
            dates.add(dd)
    return sorted(dates)

def clamp_dates(dates: Iterable[date], start: date, end: date):
    return [d for d in dates if start <= d <= end]

# ===================== SELECTOR HELPERS (PLAYWRIGHT) =====================
async def first_visible(page, locators, timeout=15000):
    for make in locators:
        try:
            loc = make(page)
            if await loc.count() > 0:
                await loc.first.wait_for(timeout=timeout)
                return loc.first
        except Exception:
            continue
    raise PWTimeoutError("No se encontró un locator válido.")

def cuerpo_locator_candidates():
    return [
        lambda p: p.get_by_label(re.compile(r"cuerpo", re.I)),
        lambda p: p.get_by_role("combobox", name=re.compile(r"cuerpo", re.I)),
        lambda p: p.locator("select[name*='cuerpo' i]"),
        lambda p: p.locator("xpath=//label[contains(.,'Cuerpo')]/following::select[1]"),
    ]

def actuantes_locator_candidates():
    return [
        lambda p: p.get_by_label(re.compile(r"actuantes|titulares", re.I)),
        lambda p: p.get_by_role("combobox", name=re.compile(r"actuantes|titulares", re.I)),
        lambda p: p.locator("select[name*='actuante' i]"),
        lambda p: p.locator("xpath=//label[contains(.,'Actuantes') or contains(.,'Titulares')]/following::select[1]"),
    ]

def fecha_locator_candidates():
    return [
        lambda p: p.get_by_label(re.compile(r"fecha", re.I)),
        lambda p: p.locator("input[type='date']"),
        lambda p: p.locator("xpath=//label[contains(.,'Fecha')]/following::input[1]"),
    ]

def hora_locator_candidates():
    return [
        lambda p: p.get_by_label(re.compile(r"hora", re.I)),
        lambda p: p.locator("input[type='time']"),
        lambda p: p.locator("xpath=//label[contains(.,'Fecha')]/following::input[@type='time'][1]"),
    ]

def buscar_locator_candidates():
    return [
        lambda p: p.get_by_role("button", name=re.compile(r"buscar", re.I)),
        lambda p: p.locator("button:has-text('BUSCAR')"),
        lambda p: p.locator("xpath=//button[contains(.,'BUSCAR') or contains(.,'Buscar')]"),
        lambda p: p.locator("input[type='submit']"),
    ]

def table_locator_candidates():
    return [
        lambda p: p.locator("table"),
        lambda p: p.locator("xpath=//table"),
    ]

def tbody_rows_locator(table):
    return table.locator("tbody tr")

# ===================== EXTRA HELPERS =====================
COL_KEYS = {
    "nombre": ["nombre", "legislador", "integrante"],
    "partido": ["lema", "partido"],
    "camara": ["cámara", "camara", "cuerpo"],
    "condicion": ["condición", "condicion", "calidad", "titular", "suplente", "actuante"],
    "sustituye": ["sustituye", "sustituido", "sustituye a"],
    "detalles": ["detalle", "detalles", "ver", "info"]
}

async def ensure_dropdown_value(select, target_text: str):
    options = await select.locator("option").all()
    for o in options:
        t = (await o.text_content() or "").strip()
        if t.lower() == target_text.lower():
            val = await o.get_attribute("value")
            await select.select_option(val)
            return
    for o in options:
        val = (await o.get_attribute("value") or "").strip()
        if val.lower() == target_text.lower():
            await select.select_option(val)
            return
    for o in options:
        t = (await o.text_content() or "").strip()
        if target_text.lower() in t.lower():
            val = await o.get_attribute("value")
            await select.select_option(val)
            return
    if options:
        val = await options[0].get_attribute("value")
        await select.select_option(val)

async def open_details_if_any(row) -> str:
    try:
        details_btn = row.locator(
            "a:has-text('Detalle'), a:has-text('Detalles'), "
            "button:has-text('Detalle'), button:has-text('Detalles')"
        )
        if await details_btn.count() == 0:
            details_btn = row.locator("[title*='Detalle'], [title*='detalle'], i[title]")
        if await details_btn.count() == 0:
            return ""
        await details_btn.first.click(timeout=ACTION_TIMEOUT)
        modal = row.page.locator(".modal.show, role=dialog")
        await modal.wait_for(timeout=ACTION_TIMEOUT)
        text = (await modal.inner_text()).strip()
        close_btn = modal.locator("button:has-text('Cerrar'), button:has-text('Close'), .btn-close")
        if await close_btn.count() > 0:
            await close_btn.first.click(timeout=ACTION_TIMEOUT)
        else:
            await row.page.mouse.click(10, 10)
        await row.page.wait_for_timeout(200)
        return text
    except PWTimeoutError:
        return ""
    except Exception:
        return ""

def pick_columns(headers: List[str]) -> Dict[str, int]:
    idx = {}
    low = [(h or "").lower().strip() for h in headers]
    for key, aliases in COL_KEYS.items():
        for i, h in enumerate(low):
            if any(a in h for a in aliases):
                idx[key] = i
                break
    return idx

# ===================== PARSE CARDS =====================
async def parse_cards(page, day_str: str, fuente_url: str):
    out = []
    name_links = page.locator("a[href*='/camarasycomisiones/legisladores/']")
    n = await name_links.count()
    if n == 0:
        return out

    for i in range(n):
        a = name_links.nth(i)

        try:
            nombre = (await a.inner_text()).strip()
        except Exception:
            nombre = ""
        if not nombre:
            continue

        cont = a.locator(
            "xpath=ancestor::*[contains(@class,'card') or contains(@class,'views-row') "
            "or contains(@class,'col') or contains(@class,'media')][1]"
        )
        if await cont.count() == 0:
            cont = a.locator("xpath=..")

        full = (await cont.inner_text()).strip()

        partido = ""
        m = re.search(r"(?:PARTIDO|LEMA)\s+([A-ZÁÉÍÓÚÑ ]{3,})", full)
        if m:
            partido = m.group(1).strip()
        else:
            lines = [ln.strip() for ln in full.splitlines() if ln.strip()]
            for ln in lines:
                if "PARTIDO " in ln.upper() or "LEMA " in ln.upper():
                    partido = re.sub(r"^(PARTIDO|LEMA)\s+", "", ln, flags=re.I).strip()
                    break

        sustit = ""
        ms = re.search(r"sustituye\s+a[:\s]+(.+)", full, re.I)
        if ms:
            sustit = ms.group(1).strip()
            sustit = re.split(r"Ver\s+Titular|Ver\s", sustit, flags=re.I)[0].strip()

        camara = ""
        full_lower = full.lower()
        if any(k in full_lower for k in ["senado","senador ","senadora ","cámara de senadores","camara de senadores"]):
            camara = "CSS"
        elif any(k in full_lower for k in ["representante","diputado","diputada","cámara de representantes","camara de representantes"]):
            camara = "CRR"
        elif re.search(r"\bcss\b", full_lower):
            camara = "CSS"
        elif re.search(r"\bcrr\b", full_lower):
            camara = "CRR"
        elif sustit:
            sust_lower = sustit.lower()
            if "senador" in sust_lower or "senadora" in sust_lower:
                camara = "CSS"
            elif any(k in sust_lower for k in ["diputado","diputada","representante"]):
                camara = "CRR"

        out.append({
            "fecha": day_str,
            "nombre": nombre,
            "partido": partido,
            "camara": camara,
            "condicion": "Actuante",
            "sustituye_a": sustit,
            "detalles_texto": "",
            "fuente_url": fuente_url
        })

    return [r for r in out if r["nombre"]]

# ===================== SCRAPE DAY =====================
async def scrape_day(page, day: date, cuerpo_form_value: str) -> List[Dict]:
    """
    - Navega a la página
    - Selecciona Cuerpo=Asamblea General, Actuantes=Actuantes
    - Carga fecha y va probando horas
    - Intenta primero tabla, luego tarjetas
    """
    await page.goto(BASE_URL, timeout=NAV_TIMEOUT)

    # seleccionar 'Cuerpo'
    cuerpo = await first_visible(page, cuerpo_locator_candidates(), timeout=ACTION_TIMEOUT)
    await ensure_dropdown_value(cuerpo, cuerpo_form_value)

    # seleccionar 'Actuantes'
    actuantes = await first_visible(page, actuantes_locator_candidates(), timeout=ACTION_TIMEOUT)
    await ensure_dropdown_value(actuantes, "Actuantes")

    # setear fecha
    fecha_input = await first_visible(page, fecha_locator_candidates(), timeout=ACTION_TIMEOUT)
    day_str = day.strftime("%Y-%m-%d")
    await fecha_input.fill(day_str)

    rows_all: List[Dict] = []

    # intentar capturar input hora si existe
    hora_input = None
    try:
        hora_input = await first_visible(page, hora_locator_candidates(), timeout=2000)
    except Exception:
        hora_input = None

    hours_to_try = HOUR_CANDIDATES if hora_input else [None]

    for hh in hours_to_try:
        if hora_input and hh:
            await hora_input.fill(hh)
            await hora_input.press("Tab")

        buscar_btn = await first_visible(page, buscar_locator_candidates(), timeout=ACTION_TIMEOUT)
        await buscar_btn.click(timeout=ACTION_TIMEOUT)

        # intentar tabla
        try:
            table = await first_visible(page, table_locator_candidates(), timeout=5000)

            ths = await table.locator("thead th").all_inner_texts()
            if not ths:
                ths = await table.locator("tr th").all_inner_texts()
            headers = [(h or "").strip() for h in ths]
            colmap = pick_columns(headers)

            rows = tbody_rows_locator(table)
            n = await rows.count()
            if n > 0:
                for i in range(n):
                    row = rows.nth(i)
                    tds = await row.locator("td").all_inner_texts()
                    if not tds:
                        continue

                    def getc(key):
                        idx = colmap.get(key)
                        return (tds[idx].replace("\n", " ").strip()
                                if idx is not None and idx < len(tds) else "")

                    detalles_texto = await open_details_if_any(row)

                    rows_all.append({
                        "fecha": day_str,
                        "nombre": getc("nombre"),
                        "partido": getc("partido"),
                        "camara": getc("camara"),
                        "condicion": getc("condicion") or "Actuante",
                        "sustituye_a": getc("sustituye"),
                        "detalles_texto": detalles_texto,
                        "fuente_url": page.url
                    })
        except PWTimeoutError:
            pass

        # si no hubo tabla con filas válidas, probamos tarjetas
        if not rows_all:
            rows_all = await parse_cards(page, day_str, page.url)

        if rows_all:
            break

    return rows_all

# ===================== MAIN LOOP =====================
async def run():
    global USE_DATES_ONLY

    df = load_existing()
    start_from = read_checkpoint() or START_DATE
    log.info(f"Inicio desde: {start_from} -> {END_DATE}")

    # construir lista de fechas reales (si existen tus archivos de fechas)
    candidate_dates: list[date] = []
    if USE_DATES_ONLY:
        dates1 = load_dates_from_jsonl(DATES_JSONL)
        dates2 = load_dates_from_csv(DATES_CSV)
        candidate_dates = clamp_dates(sorted(set(dates1) | set(dates2)), START_DATE, END_DATE)
        if not candidate_dates:
            log.warning("No encontré fechas candidatas; paso a modo diario completo.")
            USE_DATES_ONLY = False

    if USE_DATES_ONLY and candidate_dates:
        iterator = [item for item in candidate_dates if item["fecha"] >= start_from]
        log.info(f"Voy a procesar {len(iterator)} fechas candidatas de Asamblea General.")
    else:
        # fallback: día por día
        iterator = []
        d = start_from
        while d <= END_DATE:
            iterator.append(d)
            d += timedelta(days=1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="es-419", timezone_id="America/Montevideo")
        page = await context.new_page()

        for current in iterator:
            for attempt in range(1, RETRIES + 1):
                try:
                    if isinstance(current, dict):
                        day = current["fecha"]
                        cuerpo_form = current["cuerpo"]
                    else:
                        day = current
                        cuerpo_form = "Asamblea General"

                    rows = await scrape_day(page, day, cuerpo_form)

                    if rows:
                        df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
                        df.drop_duplicates(subset=["fecha", "nombre", "camara"], inplace=True)
                        df.to_csv(OUTPUT_CSV, index=False)
                        df_leg = build_legislatura_view(df)
                        df_leg.to_csv(OUTPUT_CSV_LEG, index=False)
                        log.info(f"{current} -> {len(rows)} filas nuevas (total {len(df)})")
                    else:
                        log.info(f"{current} -> sin filas")
                    break
                except Exception as e:
                    log.warning(f"{current} intento {attempt}/{RETRIES}: {e}")
                    await page.wait_for_timeout(1200 * attempt)

            if isinstance(current, dict):
                write_checkpoint(current["fecha"])
            else:
                write_checkpoint(current)

            await page.wait_for_timeout(COOLDOWN_DAY_MS)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
