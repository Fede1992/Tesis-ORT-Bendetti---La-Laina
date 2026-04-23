
"""
parliament.py
-------------
Scraper para 'Diarios de sesión' del Parlamento:
  1) Pagina el índice con ?page=0..N (o por legislatura/fechas si all_legislaturas=True)
  2) Extrae IDs y metadatos de cada fila (cuerpo, sesión, fecha, etc.)
  3) Resuelve el /IMG → PDF directo (cuando hay visor HTML)
  4) Descarga PDFs a data/raw/pdfs/
  5) Emite metadatos en JSONL: data/raw/diarios.jsonl


"""

from __future__ import annotations

import os
import re
import time
import json
import hashlib
from typing import Dict, List, Iterable, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from src.settings.settings import load_settings
from src.settings.logger import custom_logger


class ParliamentPDFScraper:
    def __init__(self) -> None:
        self.logger = custom_logger(self.__class__.__name__)

        # --- Config ---
        general = load_settings("general")
        source  = load_settings("source")
        scrape  = load_settings("scrape")

        # General
        self.output_dir  = general.get("output_dir", "data")
        self.sleep       = float(general.get("rate_sleep_seconds", 1.0))
        self.retries     = int(general.get("retries", 3))
        self.timeout     = int(general.get("timeout", 30))
        self.user_agent  = general.get("user_agent", "DebatesScraper/1.0")

        # Fuente
        self.base_index  = source["base_index"].rstrip("/")
        self.start_page  = int(source.get("start_page", 0))
        self.max_pages   = int(source.get("max_pages", 1))
        self.id_href_regex = re.compile(source.get(
            "id_href_regex",
            r"/documentosyleyes/documentos/diarios-de-sesion/(\d+)(?:$|/)"
        ))
        self.pdf_suffix  = source.get("pdf_suffix", "/IMG")

        # Scrape options (solo metadata/descarga en esta etapa)
        self.chamber_label        = scrape.get("chamber", "Parlamento Uruguay")
        self.all_legislaturas     = bool(scrape.get("all_legislaturas", False))
        self.max_per_legislatura  = int(scrape.get("max_per_legislatura", 0))  # 0 = sin límite

        # Legislaturas (value del <select>, romano y rango de fechas válidas)
        self.legislaturas_cfg = [
            {"value": "50", "romano": "L",      "desde": "2025-02-15", "hasta": "2030-02-14"},
            {"value": "49", "romano": "XLIX",   "desde": "2020-02-15", "hasta": "2025-02-14"},
            {"value": "48", "romano": "XLVIII", "desde": "2015-02-15", "hasta": "2020-02-14"},
            #{"value": "47", "romano": "XLVII",  "desde": "2010-02-15", "hasta": "2015-02-14"},
            #{"value": "46", "romano": "XLVI",   "desde": "2005-02-15", "hasta": "2010-02-14"},
            #{"value": "45", "romano": "XLV",    "desde": "2000-02-15", "hasta": "2005-02-14"},
            #{"value": "44", "romano": "XLIV",   "desde": "1995-02-15", "hasta": "2000-02-14"},
            #{"value": "43", "romano": "XLIII",  "desde": "1990-02-15", "hasta": "1995-02-14"},
            #{"value": "42", "romano": "XLII",   "desde": "1985-02-15", "hasta": "1990-02-14"},
            #{"value": "41", "romano": "XLI",    "desde": "1972-02-15", "hasta": "1973-06-27"},
            #{"value": "40", "romano": "XL",     "desde": "1967-02-15", "hasta": "1972-02-14"},
        ]

        # --- IO: carpetas de salida ---
        self.dir_pdfs = os.path.join(self.output_dir, "raw", "pdfs")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.dir_pdfs, exist_ok=True)

        # --- Sesión HTTP ---
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            # Opcional: ayuda cuando el servidor responde distinto según Accept
            "Accept": "application/pdf, text/html;q=0.9,*/*;q=0.8",
        })

        # --- Índice JSONL ---
        self.index_jsonl_path = os.path.join(self.output_dir, "raw", "diarios.jsonl")

        # Log de configuración
        self.logger.info(
            f"Config → base_index={self.base_index} "
            f"start_page={self.start_page} max_pages={self.max_pages} "
            f"all_legislaturas={self.all_legislaturas} "
            f"max_per_legislatura={self.max_per_legislatura}"
        )


    def run(self) -> List[Dict]:
        """
        Ejecuta el scraping y devuelve metadatos por documento.
        Si all_legislaturas=True, recorre todas las legislaturas con sus fechas válidas.
        Si es False, usa el índice tal cual (_iter_index_entries).
        """
        rows: List[Dict] = []

        # Elegir fuente de items (todas las legislaturas vs. índice actual)
        if self.all_legislaturas:
            iter_items = self._iter_all_legislaturas()
        else:
            iter_items = self._iter_index_entries()

        for item in iter_items:
            doc_id  = item["doc_id"]
            pdf_url = self._build_pdf_url(item["href"])

            # nombre de archivo basado en legislatura, fecha y cuerpo
            basename = self._build_basename(item)
            pdf_name = f"{basename}.pdf"
            pdf_path = os.path.join(self.dir_pdfs, pdf_name)

            # si ya existe un PDF con ese nombre, le agrego el doc_id para evitar duplicados
            if os.path.exists(pdf_path):
                basename = f"{basename}_{doc_id}"
                pdf_name = f"{basename}.pdf"
                pdf_path = os.path.join(self.dir_pdfs, pdf_name)

            # Descargar PDF
            final_url = pdf_url
            if not os.path.exists(pdf_path):
                self.logger.info(f"Descargando PDF {doc_id} -> {pdf_url}")
                try:
                    maybe_final = self._download_file(pdf_url, pdf_path, doc_id=doc_id)
                    if isinstance(maybe_final, str):
                        final_url = maybe_final
                except Exception as e:
                    self.logger.warning(f"No se pudo descargar {pdf_url}: {e}")
                    time.sleep(self.sleep)
                    continue
            else:
                self.logger.info(f"PDF ya existe: {pdf_path}")

            # Hash
            sha1 = self._sha1_file(pdf_path)

            # META final
            meta = {
                "doc_id": doc_id,
                "index_url": item.get("index_url"),
                "href": item.get("href"),
                "pdf_path": pdf_path,
                "basename": basename,
                "chamber": self.chamber_label,
                "sha1": sha1,
                # tabla del índice:
                "cuerpo":  item.get("cuerpo"),
                "sesion":  item.get("sesion"),
                "fecha":   item.get("fecha"),
                "diario":  item.get("diario"),
                "resumen": item.get("resumen"),
                # legislatura seleccionada / recorrida:
                "legislatura_value":  item.get("legislatura_value"),
                "legislatura_romano": item.get("legislatura_romano"),
                "legislatura_periodo": item.get("legislatura_periodo"),
                "filtro_desde": item.get("filtro_desde"),
                "filtro_hasta": item.get("filtro_hasta"),
            }
            rows.append(meta)
            self._append_jsonl(meta)

            time.sleep(self.sleep)

        self.logger.info(f"Total documentos procesados: {len(rows)}")
        return rows

    # ------------------ Parsing índice ------------------

    def _iter_document_ids(self) -> Iterable[Dict]:
        """
        Recorre páginas ?page=N y rinde dicts con:
        { doc_id: '6917', href: '/.../diarios-de-sesion/6917', index_url: '...page=N' }
        """
        for offset in range(self.max_pages):
            page = self.start_page + offset
            index_url = f"{self.base_index}?page={page}"
            self.logger.info(f"Index: {index_url}")

            soup = self._get_html(index_url)
            # Extraigo todos los hrefs, matcheo por regex
            seen = set()
            count_this_page = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                m = self.id_href_regex.search(href)
                if not m:
                    continue
                doc_id = m.group(1)
                if doc_id in seen:
                    continue
                seen.add(doc_id)
                count_this_page += 1
                yield {"doc_id": doc_id, "href": href, "index_url": index_url}

            self.logger.info(f"Encontrados {count_this_page} IDs en page={page}")

    def _build_pdf_url(self, href_or_id: str) -> str:
        """
        A partir de un href (relativo o absoluto) o de un ID, arma la URL final del PDF (/IMG).
        """
        # Si es sólo el ID
        if href_or_id.isdigit():
            path = f"/documentosyleyes/documentos/diarios-de-sesion/{href_or_id}{self.pdf_suffix}"
            return urljoin(self.base_index, path)

        # Si es un href genérico, extraigo el id
        m = self.id_href_regex.search(href_or_id)
        if not m:
            href_abs = urljoin(self.base_index, href_or_id)
            if not href_abs.endswith(self.pdf_suffix):
                href_abs = href_abs.rstrip("/") + self.pdf_suffix
            return href_abs

        doc_id = m.group(1)
        return self._build_pdf_url(doc_id)

    # ------------------ HTTP / IO ------------------

    def _get_html(self, url: str) -> BeautifulSoup:
        last_err = None
        for _ in range(self.retries):
            try:
                r = self.session.get(url, timeout=self.timeout)
                r.raise_for_status()
                return BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                last_err = e
                self.logger.warning(f"GET {url} falló: {e}. Reintento...")
                time.sleep(self.sleep)
        raise last_err  # type: ignore[misc]

    def _download_file(self, url: str, path: str, doc_id: Optional[str] = None) -> str:
        """
        Descarga robusta:
        - Resuelve /IMG → PDF real
        - Si baja HTML (visor), guarda copia en data/raw/html_debug y hace segundo intento parseando
        - Valida Content-Type y cabecera %PDF-
        Devuelve la URL final usada (por si difiere de la solicitada).
        """
        def _is_pdf_file(tmp_path: str, content_type: str, first_chunk: bytes) -> bool:
            is_pdf_by_ct = "pdf" in (content_type or "").lower()
            is_pdf_by_magic = first_chunk.startswith(b"%PDF-")
            return is_pdf_by_ct or is_pdf_by_magic

        def _save_debug_html(tmp_path: str, debug_name: str) -> str:
            html_dir = os.path.join(self.output_dir, "raw", "html_debug")
            os.makedirs(html_dir, exist_ok=True)
            debug_path = os.path.join(html_dir, f"{debug_name}.html")
            try:
                os.replace(tmp_path, debug_path)
            except Exception:
                try:
                    with open(tmp_path, "rb") as src, open(debug_path, "wb") as dst:
                        dst.write(src.read())
                    os.remove(tmp_path)
                except Exception:
                    pass
            return debug_path

        def _download_to_tmp(final_url: str) -> tuple[str, str, bytes]:
            with self.session.get(final_url, timeout=self.timeout, stream=True, allow_redirects=True) as r:
                r.raise_for_status()
                content_type = (r.headers.get("Content-Type") or "").lower()
                tmp_path = path + ".part"
                first_chunk = b""
                with open(tmp_path, "wb") as f:
                    first = True
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if not chunk:
                            continue
                        if first:
                            first_chunk = chunk[:8]
                            first = False
                        f.write(chunk)
            return tmp_path, content_type, first_chunk

        # 1) Primer intento resolviendo URL directa del PDF
        final_url = self._resolve_pdf_direct(url)
        last_err: Optional[Exception] = None

        for _ in range(self.retries):
            try:
                tmp_path, content_type, first_chunk = _download_to_tmp(final_url)
                if _is_pdf_file(tmp_path, content_type, first_chunk):
                    os.replace(tmp_path, path)
                    return final_url

                # HTML / visor → guardo para debug y busco candidatos
                debug_name = f"diario_{doc_id or 'unknown'}"
                debug_path = _save_debug_html(tmp_path, debug_name)
                self.logger.warning(
                    f"No es PDF (CT={content_type}) desde {final_url}. HTML guardado: {debug_path}"
                )

                # 2) Segundo intento: parsear ese HTML y buscar .pdf
                try:
                    with open(debug_path, "rb") as fh:
                        html_text = fh.read().decode("utf-8", errors="ignore")
                    soup = BeautifulSoup(html_text, "html.parser")

                    candidates: List[str] = []

                    # a) <a href="...pdf">
                    for a in soup.find_all("a", href=True):
                        href = a["href"].strip()
                        if href.lower().endswith(".pdf"):
                            candidates.append(urljoin(final_url, href))

                    # b) anchors con 'descarg' en label
                    for a in soup.find_all("a", href=True):
                        label = " ".join(filter(None, [
                            a.get_text(strip=True),
                            a.get("aria-label", ""),
                            a.get("title", "")
                        ])).lower()
                        if "descarg" in label:
                            candidates.append(urljoin(final_url, a["href"].strip()))

                    # c) onclick=window.open('...pdf')
                    for tag in soup.find_all(attrs={"onclick": True}):
                        m = re.search(r"window\.open\(['\"]([^'\"]+\.pdf)[\"']", tag["onclick"], flags=re.I)
                        if m:
                            candidates.append(urljoin(final_url, m.group(1)))

                    # d) <iframe src="...pdf">
                    for iframe in soup.find_all("iframe", src=True):
                        src = iframe["src"].strip()
                        if src.lower().endswith(".pdf"):
                            candidates.append(urljoin(final_url, src))

                    # Priorizar infolegislativa si aparece
                    pref = [c for c in candidates if "infolegislativa.parlamento.gub.uy" in c]
                    if pref:
                        final_url = pref[0]
                    elif candidates:
                        final_url = candidates[0]
                    else:
                        raise RuntimeError("No se encontró enlace a PDF en el HTML del visor.")

                    # Reintento con candidata
                    tmp_path2, ct2, first2 = _download_to_tmp(final_url)
                    if _is_pdf_file(tmp_path2, ct2, first2):
                        os.replace(tmp_path2, path)
                        return final_url

                    os.remove(tmp_path2)
                    raise RuntimeError(f"Segundo intento tampoco es PDF (CT={ct2}).")

                except Exception as e:
                    last_err = e
                    self.logger.warning(f"Resolución secundaria de PDF falló: {e}")

            except Exception as e:
                last_err = e
                self.logger.warning(f"Descarga {final_url} falló: {e}.")

            time.sleep(self.sleep)

        if last_err:
            raise last_err
        raise RuntimeError("Descarga fallida (razón desconocida)")

    @staticmethod
    def _sha1_file(path: str) -> Optional[str]:
        try:
            h = hashlib.sha1()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            return h.hexdigest()
        except Exception:
            return None

    # ------------------ JSONL índice ------------------

    def _append_jsonl(self, row: Dict) -> None:
        try:
            with open(self.index_jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            self.logger.error(f"No se pudo escribir en {self.index_jsonl_path}: {e}")

    # ------------------ Resolución de /IMG a PDF directo ------------------

    def _resolve_pdf_direct(self, url_img: str) -> str:
        """
        Resuelve la URL directa del PDF a partir de una URL /IMG (visor HTML).
        - Prioriza anchors a infolegislativa.parlamento.gub.uy que terminen en .pdf
        - Usa botón 'Descargue el archivo...' si existe
        - Soporta svg#baseSvg → padre <a>, onclick con window.open('...pdf'), meta refresh, iframes
        """
        try:
            # 1) HEAD por si redirige directo
            h = self.session.head(url_img, timeout=self.timeout, allow_redirects=True)
            ct = (h.headers.get("Content-Type") or "").lower()
            if "pdf" in ct or (h.url and h.url.lower().endswith(".pdf")):
                return h.url

            # 2) GET HTML
            r = self.session.get(url_img, timeout=self.timeout, allow_redirects=True)
            r.raise_for_status()
            ct = (r.headers.get("Content-Type") or "").lower()
            if "html" not in ct:
                return r.url or url_img

            soup = BeautifulSoup(r.text, "html.parser")

            def is_pdf(href: str) -> bool:
                return href and href.lower().endswith(".pdf")

            def abs_url(href: str) -> str:
                return urljoin(r.url, href)

            # (a) botón con 'descarg' en label y que apunte a .pdf
            for a in soup.find_all("a", href=True):
                label = " ".join(filter(None, [
                    a.get_text(strip=True),
                    a.get("aria-label", ""),
                    a.get("title", "")
                ])).lower()
                href = a["href"].strip()
                if "descarg" in label and is_pdf(href):
                    return abs_url(href)

            # (b) anchors a infolegislativa .pdf
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if is_pdf(href):
                    parsed = urlparse(abs_url(href))
                    if "infolegislativa.parlamento.gub.uy" in parsed.netloc:
                        return parsed.geturl()

            # (c) cualquier .pdf que NO sea dominio 'legislativo.parlamento.gub.uy' (enciclopedia)
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if is_pdf(href):
                    parsed = urlparse(abs_url(href))
                    if "legislativo.parlamento.gub.uy" not in parsed.netloc:
                        return parsed.geturl()

            # (d) svg#baseSvg → padre <a>
            svg = soup.select_one("svg#baseSvg")
            if svg:
                a_parent = svg.find_parent("a", href=True)
                if a_parent and a_parent["href"].strip():
                    return abs_url(a_parent["href"].strip())

            # (e) onclick con window.open('...pdf')
            for tag in soup.find_all(attrs={"onclick": True}):
                m = re.search(r"window\.open\(['\"]([^'\"]+\.pdf)[\"']", tag["onclick"], flags=re.I)
                if m:
                    return abs_url(m.group(1))

            # (f) meta refresh
            meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
            if meta and meta.get("content"):
                m = re.search(r"url=(.+)", meta["content"], flags=re.I)
                if m:
                    return abs_url(m.group(1).strip())

            # (g) scripts con URL .pdf
            for sc in soup.find_all("script"):
                txt = sc.string or sc.text or ""
                m = re.search(r"https?://[^\s\"']+\.pdf", txt, flags=re.I)
                if m:
                    return m.group(0)

            self.logger.warning(f"No encontré enlace PDF claro en {url_img}. Intento directo.")
            return r.url or url_img

        except Exception as e:
            self.logger.warning(f"No se pudo resolver PDF directo desde {url_img}: {e}. Uso URL original.")
            return url_img

    # ------------------ Helpers de nombres/fechas ------------------

    def _slugify_filename(self, text: str, max_length: int = 80) -> str:
        if not text:
            return "sin_resumen"
        txt = text.strip()
        txt = re.sub(r"[^a-zA-Z0-9áéíóúÁÉÍÓÚñÑ]+", "_", txt)
        txt = re.sub(r"_+", "_", txt)
        txt = txt[:max_length].rstrip("_")
        return txt or "sin_resumen"

    def _normalize_date(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        s = raw.strip()
        # YYYY-MM-DD
        m = re.match(r"^(\d{4})[-/](\d{2})[-/](\d{2})$", s)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        # DD/MM/YYYY o DD-MM-YYYY
        m = re.match(r"^(\d{2})[-/](\d{2})[-/](\d{4})$", s)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    def _build_basename(self, item: Dict) -> str:
        """
        <legislatura_value>_<YYYY-MM-DD>_<cuerpo>
        Todo en minúsculas. Ej: 49_2023-11-12_crr
        """
        leg = (item.get("legislatura_value") or "").strip()
        fecha = self._normalize_date(item.get("fecha"))
        cuerpo = (item.get("cuerpo") or "").strip().lower().replace(" ", "_")

        parts = [p for p in (leg, fecha, cuerpo) if p]
        return "_".join(parts)

    # ------------------ Índice (sin filtros) ------------------

    def _iter_index_entries(self) -> Iterable[Dict]:
        """
        Recorre el índice de 'Diarios de sesiones' y rinde dicts con:
        { doc_id, href (/IMG), index_url, cuerpo, sesion, fecha, diario, resumen,
          legislatura_value, legislatura_text, legislatura_romano, legislatura_periodo }
        """
        for offset in range(self.max_pages):
            page = self.start_page + offset
            index_url = f"{self.base_index}?page={page}"
            self.logger.info(f"Index: {index_url}")

            soup = self._get_html(index_url)

            # Legislatura seleccionada en ESTA página
            leg = self._extract_legislatura(soup, index_url)

            # Ubicar fila de encabezados por texto conocido
            header_row = None
            for tr in soup.find_all("tr"):
                hdr_txt = " ".join(td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])).strip().lower()
                if ("cuerpo" in hdr_txt and "sesión" in hdr_txt and "fecha" in hdr_txt
                    and "diario" in hdr_txt and "resumen" in hdr_txt):
                    header_row = tr
                    break
            if not header_row:
                self.logger.warning("No se encontró la fila de encabezados en el índice.")
                return

            table = header_row.find_parent("table")
            if not table:
                self.logger.warning("No se encontró la tabla de resultados en el índice.")
                return

            count_this_page = 0
            for tr in table.find_all("tr"):
                if tr is header_row:
                    continue
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue

                cuerpo  = tds[0].get_text(" ", strip=True) or None
                sesion  = tds[1].get_text(" ", strip=True) or None
                fecha   = tds[2].get_text(" ", strip=True) or None
                diario  = tds[3].get_text(" ", strip=True) or None
                resumen = tds[4].get_text(" ", strip=True) or None

                # Link [PDF] en la misma fila
                href_pdf = None
                for a in tr.find_all("a", href=True):
                    label = a.get_text(" ", strip=True).lower()
                    if "pdf" in label:
                        href_pdf = a["href"].strip()
                        break
                if not href_pdf:
                    continue

                abs_href = urljoin(index_url, href_pdf)
                m = self.id_href_regex.search(abs_href)
                if not m:
                    self.logger.warning(f"No se pudo extraer doc_id desde href={abs_href}")
                    continue

                doc_id = m.group(1)

                yield {
                    "doc_id": doc_id,
                    "href": abs_href,       # .../diarios-de-sesion/<id>/IMG
                    "index_url": index_url,
                    "cuerpo": cuerpo,
                    "sesion": sesion,
                    "fecha": fecha,
                    "diario": diario,
                    "resumen": resumen,
                    "legislatura_value": leg.get("legislatura_value"),
                    "legislatura_text": leg.get("legislatura_text"),
                    "legislatura_romano": leg.get("legislatura_romano"),
                    "legislatura_periodo": leg.get("legislatura_periodo"),
                }

                count_this_page += 1

            self.logger.info(f"Encontrados {count_this_page} ítems en page={page}")

    # ------------------ Legislatura seleccionada en página ------------------

    def _extract_legislatura(self, soup: BeautifulSoup, index_url: str) -> dict:
        """
        Lee la legislatura seleccionada desde el <select id="edit-lgl-nro"> del índice.
        Devuelve:
        {
            "legislatura_value": "50",
            "legislatura_text": "-Legislatura Actual- L (2025-2030)",
            "legislatura_romano": "L",
            "legislatura_periodo": "2025-2030"
        }
        Si no detecta nada, devuelve valores None.
        """
        out = {
            "legislatura_value": None,
            "legislatura_text": None,
            "legislatura_romano": None,
            "legislatura_periodo": None,
        }

        try:
            sel = soup.find("select", id="edit-lgl-nro")
            selected_opt = None

            if sel:
                # 1) Opción marcada como selected="selected"
                selected_opt = sel.find("option", selected=True)

                # 2) Si no hay 'selected', intentamos por querystring ?Lgl_Nro=...
                if not selected_opt:
                    qs = parse_qs(urlparse(index_url).query)
                    wanted = (qs.get("Lgl_Nro") or [None])[0]
                    if wanted:
                        selected_opt = sel.find("option", attrs={"value": wanted})

                # 3) Si sigue sin aparecer, y hay 'All', tomamos 'All' como fallback
                if not selected_opt and sel.find("option", attrs={"value": "All"}):
                    selected_opt = sel.find("option", attrs={"value": "All"})

            if selected_opt:
                value = (selected_opt.get("value") or "").strip() or None
                text  = (selected_opt.get_text(" ", strip=True) or None)

                out["legislatura_value"] = value
                out["legislatura_text"]  = text

                # Ejemplo de texto:
                # "-Legislatura Actual- L (2025-2030)"  |  "XLIX (2020-2025)"
                if text:
                    # Periodo entre paréntesis
                    m_periodo = re.search(r"\((\d{4}\s*-\s*\d{4})\)", text)
                    if m_periodo:
                        out["legislatura_periodo"] = m_periodo.group(1).replace(" ", "")

                    # Romano: última "palabra" de mayúsculas latinas
                    m_rom = re.search(r"\b([IVXLCDM]+)\b", text)
                    if m_rom:
                        out["legislatura_romano"] = m_rom.group(1)

        except Exception as e:
            self.logger.warning(f"No se pudo extraer legislatura: {e}")

        return out

    # ------------------ Índice (con filtros por legislatura/fechas) ------------------

    def _get_html_with_params(self, url: str, params: Dict[str, str]) -> BeautifulSoup:
        last_err = None
        for _ in range(self.retries):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                return BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                last_err = e
                self.logger.warning(f"GET {url} params={params} falló: {e}. Reintento...")
                time.sleep(self.sleep)
        raise last_err  # type: ignore[misc]

    def _iter_index_entries_with_filters(self, lgl_value: str, fecha_desde: str, fecha_hasta: str) -> Iterable[Dict]:
        """
        Recorre el índice filtrado por legislatura y fechas y rinde dicts con:
        { doc_id, href (/IMG), index_url, cuerpo, sesion, fecha, diario, resumen,
          legislatura_value, legislatura_romano, legislatura_periodo, filtro_desde, filtro_hasta }
        """
        for offset in range(self.max_pages):
            page = self.start_page + offset
            params = {
                "page": str(page),
                "Lgl_Nro": lgl_value,
                "fecha_desde": fecha_desde,
                "fecha_hasta": fecha_hasta,
            }
            index_url = f"{self.base_index}"
            self.logger.info(f"Index (filtered): {index_url} params={params}")

            soup = self._get_html_with_params(index_url, params)

            # Encabezados Cuerpo · Sesión · Fecha · Diario · Resumen
            header_row = None
            for tr in soup.find_all("tr"):
                hdr_txt = " ".join(td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])).strip().lower()
                if ("cuerpo" in hdr_txt and "sesión" in hdr_txt and "fecha" in hdr_txt
                    and "diario" in hdr_txt and "resumen" in hdr_txt):
                    header_row = tr
                    break
            if not header_row:
                self.logger.warning("No se encontró la fila de encabezados en el índice (filtrado).")
                return

            table = header_row.find_parent("table")
            if not table:
                self.logger.warning("No se encontró la tabla de resultados en el índice (filtrado).")
                return

            count_this_page = 0
            for tr in table.find_all("tr"):
                if tr is header_row:
                    continue
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue

                cuerpo  = tds[0].get_text(" ", strip=True) or None
                sesion  = tds[1].get_text(" ", strip=True) or None
                fecha   = tds[2].get_text(" ", strip=True) or None
                diario  = tds[3].get_text(" ", strip=True) or None
                resumen = tds[4].get_text(" ", strip=True) or None

                href_pdf = None
                for a in tr.find_all("a", href=True):
                    label = a.get_text(" ", strip=True).lower()
                    if "pdf" in label:
                        href_pdf = a["href"].strip()
                        break
                if not href_pdf:
                    continue

                abs_href = urljoin(index_url, href_pdf)
                m = self.id_href_regex.search(abs_href)
                if not m:
                    self.logger.warning(f"No se pudo extraer doc_id desde href={abs_href}")
                    continue
                doc_id = m.group(1)

                yield {
                    "doc_id": doc_id,
                    "href": abs_href,       # .../diarios-de-sesion/<id>/IMG
                    "index_url": f"{index_url}?page={page}",
                    "cuerpo": cuerpo,
                    "sesion": sesion,
                    "fecha": fecha,
                    "diario": diario,
                    "resumen": resumen,
                    "legislatura_value": lgl_value,
                    "filtro_desde": fecha_desde,
                    "filtro_hasta": fecha_hasta,
                }

                count_this_page += 1

            self.logger.info(f"Encontrados {count_this_page} ítems en page={page} (Lgl={lgl_value})")

    def _iter_all_legislaturas(self) -> Iterable[Dict]:
        """
        Itera todas las legislaturas definidas en self.legislaturas_cfg aplicando sus rangos
        de fechas válidos, y opcionalmente limita a N items por legislatura según
        self.max_per_legislatura.
        """
        for leg in self.legislaturas_cfg:
            lgl_value   = leg["value"]
            fecha_desde = leg["desde"]
            fecha_hasta = leg["hasta"]

            count = 0
            for item in self._iter_index_entries_with_filters(lgl_value, fecha_desde, fecha_hasta):
                # Completar campos “derivables”
                item["legislatura_romano"] = leg["romano"]
                # periodo legible (normalizado)
                if leg["value"] == "41":
                    item["legislatura_periodo"] = "1972-1973"
                elif leg["value"] == "40":
                    item["legislatura_periodo"] = "1967-1972"
                elif leg["value"] == "42":
                    item["legislatura_periodo"] = "1985-1990"
                elif leg["value"] == "43":
                    item["legislatura_periodo"] = "1990-1995"
                elif leg["value"] == "44":
                    item["legislatura_periodo"] = "1995-2000"
                elif leg["value"] == "45":
                    item["legislatura_periodo"] = "2000-2005"
                elif leg["value"] == "46":
                    item["legislatura_periodo"] = "2005-2010"
                elif leg["value"] == "47":
                    item["legislatura_periodo"] = "2010-2015"
                elif leg["value"] == "48":
                    item["legislatura_periodo"] = "2015-2020"
                elif leg["value"] == "49":
                    item["legislatura_periodo"] = "2020-2025"
                elif leg["value"] == "50":
                    item["legislatura_periodo"] = "2025-2030"

                yield item
                count += 1

                if self.max_per_legislatura and count >= self.max_per_legislatura:
                    self.logger.info(
                        f"Tope alcanzado: {count} ítems para Legislatura {leg['romano']} (value={lgl_value})."
                    )
                    break


# Ejecución directa simple (usa settings/*.yaml)
if __name__ == "__main__":
    scraper = ParliamentPDFScraper()
    scraper.run()
