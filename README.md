# Análisis del Discurso Parlamentario Uruguayo usando Técnicas de Procesamiento de Lenguaje Natural

Tesis de Maestría en Big Data — Universidad ORT Uruguay
**Autores:** Francisco Benedetti · Federico La Laina
**Tutor:** Diego Jauré

---

## Resumen

Este trabajo tiene como propósito principal **mejorar y comparar el rendimiento de dos modelos de lenguaje** para la evaluación automática del sentimiento en intervenciones parlamentarias uruguayas, mediante *transfer learning* sobre un corpus etiquetado manualmente.

El proyecto se estructura en torno a dos objetivos:

### 1. Comparación de modelos mediante transfer learning

Se aplica transfer learning sobre dos modelos basados en la arquitectura *transformer*, ajustándolos a un corpus de intervenciones parlamentarias uruguayas etiquetadas manualmente en tres categorías de sentimiento (**Positivo, Negativo, Neutro**):

- **RoBERTuito** (Pérez et al.) — preentrenado con texto informal de diversas variedades del español.
- **ROUBERTa** (Filevich et al.) — preentrenado exclusivamente con prensa uruguaya.

Se evalúan los modelos base y sus versiones ajustadas para:
- cuantificar el impacto del *fine-tuning* sobre datos parlamentarios, y
- determinar si la proximidad lingüística y cultural del corpus de preentrenamiento de ROUBERTa se traduce en una ventaja adicional tras el *fine-tuning*.

### 2. Construcción de una base de datos parlamentaria reutilizable

Se desarrolla un corpus sistemático de intervenciones parlamentarias uruguayas del período **2010 – setiembre 2025**, extraídas de las actas oficiales del Parlamento y abarcando **Cámara de Senadores** y **Cámara de Representantes**. El proceso incluye scraping, limpieza y normalización automatizados, con metadatos homogéneos (fecha, legislatura, cámara, legislador, sección temática, extensión, etc.).

La base de datos y el código se liberan en GitHub como aporte a la comunidad académica.

---

## Estructura del repositorio

```
.
├── README.md
├── requirements.txt
├── src/
│   ├── main.py                    # Entry point del scraper de diarios
│   ├── settings/                  # Carga de config (config.yml) y logger
│   ├── scrapers/
│   │   ├── parliament.py          # Scraper de diarios de sesión (PDFs)
│   │   └── parlamentarios.py      # Scraper de legisladores actuantes (Playwright)
│   ├── refining/
│   │   ├── pdf_to_text.py         # Conversión PDF → TXT con reflujo de columnas
│   │   └── clean_texts.py         # Limpieza de encabezados, índices, nóminas
│   ├── parsers/
│   │   └── parse_intervenciones.py  # Segmentación de oradores e intervenciones
│   ├── tokenization/
│   │   └── tokenizer.py
│   ├── structs/
│   │   └── transcripts.py         # Dataclasses del dominio
│   └── eda/
│       └── df.py                  # Helper: carga txt_refined como DataFrame
├── notebooks/                     # Notebooks de análisis exploratorio y modelado (ejecutar en orden)
│   ├── 01_eda_general.ipynb
│   ├── 02_eda_css.ipynb
│   ├── 03_eda_parlamentarios.ipynb
│   ├── 04_imputacion_partidos.ipynb
│   ├── 05_eda_muestras_etiquetadas.ipynb
│   ├── 06_modelo_v8_robertuito.ipynb  # Fine-tuning RoBERTuito (Colab)
│   ├── 07_modelo_v8_rouberta.ipynb    # Fine-tuning ROUBERTa (Colab)
│   ├── 08_inferencia_completa.ipynb   # Inferencia sobre el corpus completo
│   └── 09_bootstrap.ipynb             # Evaluación estadística por bootstrap pareado
├── data/                          # Datos (ignorado por git — ver data/README.md)
│   ├── raw/                       # Crudos: pdfs/, html_debug/, diarios.jsonl, parlamentarios/
│   ├── external/                  # Fuentes externas
│   ├── interim/                   # Intermedios: texts/, txt_refined/, interim_jsonl/, debates_*.jsonl
│   ├── labeled/                   # Muestras etiquetadas manualmente
│   ├── processed/                 # Datasets finales (df_final_*.jsonl)
│   ├── logs/
│   └── README.md                  # Documentación detallada de cada archivo
└── robertuito_finetuned_parlamento/  # Modelo fine-tuned (artefactos de Trainer)
```

---

## Pipeline end-to-end

El flujo de procesamiento consta de nueve etapas secuenciales:

| # | Etapa | Módulo | Input | Output |
|---|-------|--------|-------|--------|
| 1 | **Scraping** | `src/scrapers/parliament.py` | `parlamento.gub.uy` | `data/raw/pdfs/*.pdf`, `data/raw/diarios.jsonl` |
| 2 | **Scraping legisladores** | `src/scrapers/parlamentarios.py` | Asamblea General (Playwright) | `data/raw/parlamentarios/*.csv` |
| 3 | **PDF → texto** | `src/refining/pdf_to_text.py` | `data/raw/pdfs/` | `data/interim/texts/` |
| 4 | **Limpieza** | `src/refining/clean_texts.py` | `data/interim/texts/` | `data/interim/txt_refined/` |
| 5 | **Parseo de intervenciones** | `src/parsers/parse_intervenciones.py` | `data/interim/txt_refined/` | `data/interim/interim_jsonl/` |
| 6 | **Consolidación + etiquetado + EDA** | `notebooks/01–05` | `data/interim/` + `data/raw/parlamentarios/` + `data/labeled/` | `data/processed/df_final_*.jsonl` |
| 7 | **Fine-tuning de sentimiento** | `notebooks/06–07` | `data/processed/df_final_para_modelo.jsonl` | `analisis_*.xlsx`, `robertuito_finetuned_parlamento/` |
| 8 | **Bootstrap estadístico** | `notebooks/09_bootstrap.ipynb` | `analisis_*.xlsx` (notebooks 06–07) | Intervalos de confianza y comparaciones pareadas |
| 9 | **Inferencia completa** | `notebooks/08_inferencia_completa.ipynb` | `df_final_para_modelo.jsonl` + modelo fine-tuned | `df_final_con_sentimiento.jsonl/.csv` |

---

## Instalación

**Requisitos:** Python 3.10+

```bash
# Clonar el repo
git clone https://github.com/Fede1992/Tesis-ORT-Benedetti-La-Laina.git
cd Tesis-ORT-Benedetti-La-Laina

# Crear entorno virtual
python -m venv .venv
source .venv/bin/activate      # Linux / macOS
.venv\Scripts\activate         # Windows

# Instalar dependencias
pip install -r requirements.txt
```

> ℹ️ Los notebooks `06` y `07` (fine-tuning) fueron ejecutados en Google Colab con GPU. Para reproducirlos localmente se requiere GPU con CUDA y suficiente VRAM.

---

## Cómo reproducir

### 1. Scrapear diarios de sesión

Configurar `src/settings/config.yml` (ver sección *Configuración*) y ejecutar:

```bash
python -m src.main
```

Esto descarga los PDFs a `data/raw/pdfs/`, genera `data/raw/diarios.jsonl` con metadata de cada diario, y consolida en `data/interim/debates.jsonl`.

### 2. Scrapear nómina de legisladores

```bash
playwright install chromium      # una sola vez
python -m src.scrapers.parlamentarios
```

### 3. Convertir PDFs a texto

```bash
python -m src.refining.pdf_to_text
```

### 4. Limpiar textos

```bash
python -m src.refining.clean_texts
```

### 5. Parsear intervenciones

```bash
python -m src.parsers.parse_intervenciones
```

### 6. EDA, consolidación e imputación de partidos

Ejecutar los notebooks en `notebooks/` en el siguiente orden:

1. `01_eda_general.ipynb` — análisis general del corpus
2. `02_eda_css.ipynb` — análisis de Cámara de Senadores
3. `03_eda_parlamentarios.ipynb` — análisis de la nómina
4. `04_imputacion_partidos.ipynb` — imputación de partido político por legislador
5. `05_eda_muestras_etiquetadas.ipynb` — análisis de la muestra etiquetada manualmente

Salida: `data/processed/df_final_para_modelo.jsonl`

### 7. Fine-tuning de sentimiento (Colab)

Ejecutar en Google Colab (requieren GPU):

- `06_modelo_v8_robertuito.ipynb` — fine-tuning de RoBERTuito
- `07_modelo_v8_rouberta.ipynb` — fine-tuning de ROUBERTa

Cada notebook genera un archivo `analisis_*.xlsx` con las predicciones sobre el test set y guarda el modelo fine-tuned.

### 8. Evaluación estadística por bootstrap

```bash
# Ejecutar localmente (no requiere GPU)
notebooks/09_bootstrap.ipynb
```

Toma como input los `analisis_*.xlsx` generados en el paso anterior y produce los intervalos de confianza al 95 % (1.000 remuestreos) y las comparaciones pareadas entre configuraciones reportadas en la tesis.

### 9. Inferencia sobre el corpus completo (Colab)

Ejecutar en Google Colab:

- `08_inferencia_completa.ipynb` — predice sentimiento para las 23.120 intervenciones usando el modelo fine-tuned de RoBERTuito (sliding-window mean, seed 2050)

Salida: `df_final_con_sentimiento.jsonl` / `.csv`

---

## Configuración

Los parámetros del pipeline viven en `src/settings/config.yml`:

| Sección | Clave | Descripción |
|---------|-------|-------------|
| `general` | `output_dir` | Carpeta de salida de datos (por defecto `data`) |
| `general` | `rate_sleep_seconds` | Delay entre requests al scrapear |
| `general` | `retries`, `timeout` | Reintentos y timeout HTTP |
| `source` | `base_index` | URL índice de diarios de sesión |
| `source` | `max_pages` | Máximo de páginas del índice a recorrer |
| `scrape` | `all_legislaturas` | Si recorre todas las legislaturas disponibles |
| `export` | `jsonl_filename`, `csv_filename` | Nombres de los archivos de salida |

---

## Dataset

El corpus final contiene las intervenciones parlamentarias uruguayas 2010–2025 con los siguientes campos principales:

- `fecha`, `legislatura`, `camara` — temporalidad y cámara
- `legislador`, `partido` — orador (partido imputado cuando falta)
- `seccion`, `titulo_seccion` — tópico de la intervención dentro de la sesión
- `intervencion` — texto normalizado
- `sentimiento` — etiqueta manual (`N`, `Neu`, `P`) para la muestra etiquetada (1.224 filas)
- `sentimiento_pred` — predicción del modelo para las 23.120 intervenciones
- `pred_conf` — confianza de la predicción (0–1)
- `prob_N`, `prob_Neu`, `prob_P` — probabilidades por clase

### Descarga

El dataset completo con sentimiento predicho está disponible en los [**Releases del repositorio**](https://github.com/Fede1992/Tesis-ORT-Benedetti-La-Laina/releases/tag/v1.0):

| Archivo | Descripción | Filas |
|---------|-------------|-------|
| `df_final_con_sentimiento.csv` | Dataset completo con predicciones de sentimiento | 23.120 |
| `df_intervenciones_etiquetadas_manual.csv` | Muestra etiquetada manualmente (corpus de entrenamiento) | 1.224 |

> Los archivos `data/` están ignorados por git por su tamaño. Ver `data/README.md` para el detalle completo de cada archivo.

---

## Modelos evaluados

| Modelo | Checkpoint | Preentrenamiento |
|--------|-----------|------------------|
| RoBERTuito | `pysentimiento/robertuito-sentiment-analysis` | Texto informal en español (multi-variedad) |
| ROUBERTa | `pln-udelar/rouberta-base-uy22-cased` | Prensa uruguaya (UY22) |

---

## Contacto

Para consultas sobre la tesis, acceso al corpus o colaboraciones:

- Francisco Benedetti - fbenedetti97@gmail.com
- Federico La Laina - flalaina@gmail.com
