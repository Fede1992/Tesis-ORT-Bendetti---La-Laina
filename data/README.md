# `data/` — Estructura del corpus

Esta carpeta contiene todos los datos del proyecto. Sigue la convención estándar de Data Science (raw / interim / processed) para separar claramente cada etapa del pipeline.

> **Nota:** los archivos pesados están ignorados por git (ver `.gitignore`). Solo este `README.md` queda versionado. Para obtener el corpus completo, contactar a los autores.

---

## Estructura

```
data/
├── raw/           # Datos crudos, tal como salen de la fuente — no editar
├── external/      # Datos de fuentes externas al pipeline
├── interim/       # Datos intermedios generados por el pipeline
├── labeled/       # Muestras etiquetadas manualmente
├── processed/     # Datasets finales listos para modelado / análisis
├── logs/          # Logs de ejecución
└── README.md      # Este archivo
```

---

## `raw/` — Datos crudos

Salida directa de los scrapers. **Inmutable:** no se edita a mano.

| Archivo / carpeta | Descripción | Generado por |
|---|---|---|
| `pdfs/` | PDFs de diarios de sesión (formato `<legislatura>_<fecha>_<cuerpo>.pdf`) | `src/scrapers/parliament.py` |
| `html_debug/` | Copias HTML de páginas del parlamento cuando el scraper no encontró el PDF directo | `src/scrapers/parliament.py` |
| `diarios.jsonl` | Metadata de cada diario scrapeado (id, url, fecha, cuerpo, legislatura, etc.) | `src/scrapers/parliament.py` |
| `parlamentarios/` | Nómina de legisladores actuantes por legislatura (Asamblea General) | `src/scrapers/parlamentarios.py` |

---

## `external/` — Fuentes externas

| Archivo | Descripción |
|---|---|
| `presidencia_intervenciones.xlsx` | Dataset externo con intervenciones de presidencia (complemento manual) |

---

## `interim/` — Datos intermedios

Outputs de cada etapa del pipeline antes del dataset final.

| Archivo / carpeta | Descripción | Generado por |
|---|---|---|
| `texts/` | Texto extraído de cada PDF (un `.txt` por sesión) | `src/refining/pdf_to_text.py` |
| `txt_refined/` | Texto limpio (sin encabezados, índices, nóminas) por cámara | `src/refining/clean_texts.py` |
| `interim_jsonl/` | Intervenciones parseadas por sesión (un `.jsonl` por sesión + cámara) | `src/parsers/parse_intervenciones.py` |
| `debates.jsonl` | Metadata consolidada de todas las sesiones | `src/main.py` |
| `debates_ag.jsonl` | Intervenciones de Asamblea General consolidadas | Notebooks de EDA |
| `debates_cp.jsonl` | Intervenciones de Comisión Permanente consolidadas | Notebooks de EDA |
| `debates_crr.jsonl` | Intervenciones de Cámara de Representantes consolidadas | Notebooks de EDA |
| `debates_css.jsonl` | Intervenciones de Cámara de Senadores consolidadas | Notebooks de EDA |
| `debates_clean_css.jsonl` | Versión limpia de CSS | Notebooks de EDA |

---

## `labeled/` — Muestras etiquetadas

Subconjuntos con etiquetas manuales de sentimiento.

| Archivo | Descripción |
|---|---|
| `muestras_css_para_etiquetar.{csv,jsonl}` | Muestra estratificada de CSS seleccionada para etiquetado manual |
| `muestras_css_etiquetadas.csv` | Muestra de CSS con etiquetas manuales (N/Neu/P) |
| `muestra_300_para_etiquetar.xlsx` | 300 intervenciones adicionales seleccionadas para etiquetado |
| `muestra_300_etiquetadas.xlsx` | 300 intervenciones con etiquetas manuales |

---

## `processed/` — Datasets finales

Datasets consolidados y listos para análisis o modelado.

| Archivo | Descripción | Usado por |
|---|---|---|
| `df_final.jsonl` | Dataset limpio completo de intervenciones | `src/tokenization/tokenizer.py`, `eda_css.ipynb` |
| `df_final.csv` | Igual que el anterior en formato CSV | `imputacion_partidos.ipynb` |
| `df_final_etiquetados.jsonl` | Dataset con partidos imputados y etiquetas cruzadas | `eda_muestras_etiquetadas.ipynb` |
| `df_final_para_modelo.jsonl` | **Dataset final para fine-tuning de sentimiento** | `src/modeling/sentimiento.py` |
| `_archive/` | Versiones previas e intermedios descartados (no usar) | — |

---

## `logs/` — Logs de ejecución

Logs generados por scrapers y otros scripts (`.log` por fecha).

---

## Flujo de datos (resumen)

```
                                    [EDA + etiquetado manual]
                                            |
raw/pdfs/ ──► interim/texts/ ──► interim/txt_refined/ ──► interim/interim_jsonl/
     │                                                            │
     └──► raw/diarios.jsonl                                        │
                                                                   ▼
 raw/parlamentarios/ ───────────► [consolidación + imputación] ──► processed/df_final*.jsonl
                                                                   │
                                                                   ▼
                                                            [fine-tuning modelo]
                                                                   │
                                                                   ▼
                                                     robertuito_finetuned_parlamento/
```
