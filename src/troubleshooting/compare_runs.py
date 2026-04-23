#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compara múltiples CSVs de debug generados por clean_texts.py --debug-csv,
y produce tablas con valores y deltas por archivo y por métrica.

Uso típico:
    python -m src.troubleshooting.compare_runs \
        data/debug/run_2025-10-23.csv data/debug/run_2025-10-24.csv \
        --labels "r1_2025-10-23" "r2_2025-10-24" \
        --baseline r1_2025-10-23 \
        --out-dir troubleshooting/out \
        --topn 20

Si no pasás --labels, usa el stem del archivo como etiqueta.
El baseline por defecto es la PRIMERA corrida listada.
"""

from __future__ import annotations
import argparse
from pathlib import Path
import sys
from typing import List, Optional, Dict
import pandas as pd

DEFAULT_METRICS_ORDER = [
    "n_legal_blocks",
    "len_01_norm_unicode", "len_02_dehyphen", "len_03_strip_summary",
    "len_04_smart_join", "len_05_cut_from_od", "len_06_norm_orden_dia",
    "len_07_strip_roster_stage", "len_08_strip_procedural", "len_09_postfix",
    "len_10_after_legal_cut", "len_11_final",
    "ratio_after_summary", "ratio_after_procedural", "ratio_after_legal",
]

def _expand_csv_args(args_list: List[str], pattern: str) -> List[Path]:
    """
    Acepta rutas a archivos, globs y carpetas.
    - Si es carpeta: toma todos los CSVs con 'pattern' dentro (no recursivo).
    - Si es glob: expande a archivos.
    - Si es archivo: lo devuelve tal cual.
    Retorna una lista de Paths (uno por corrida).
    """
    out: List[Path] = []
    for raw in args_list:
        p = Path(raw)
        if p.is_dir():
            found = sorted(p.glob(pattern))
            if not found:
                print(f"[WARN] Carpeta sin CSVs que hagan match con {pattern}: {p}", file=sys.stderr)
            else:
                # por convención, toma el PRIMER CSV de la carpeta como la corrida
                out.append(found[0])
        else:
            # soporta globs pasados como texto
            if any(ch in raw for ch in "*?[]"):
                matches = sorted(Path(".").glob(raw))
                if not matches:
                    print(f"[WARN] Glob sin matches: {raw}", file=sys.stderr)
                else:
                    out.extend(matches)
            else:
                if not p.exists():
                    print(f"[WARN] No existe: {p}", file=sys.stderr)
                else:
                    out.append(p)
    # quitar duplicados preservando orden
    seen = set()
    uniq = []
    for x in out:
        if x.resolve() not in seen:
            uniq.append(x)
            seen.add(x.resolve())
    return uniq

def _labels_from_paths(paths: List[Path], use_parent: bool) -> List[str]:
    """
    Si use_parent=True: usa el nombre de la carpeta contenedora como etiqueta.
    Si False: usa el stem del archivo.
    En caso de colisión, agrega sufijos _2, _3, ...
    """
    labels: List[str] = []
    counts: Dict[str, int] = {}
    for p in paths:
        base = (p.parent.name if use_parent else p.stem) or p.stem
        n = counts.get(base, 0)
        lab = base if n == 0 else f"{base}_{n+1}"
        counts[base] = n + 1
        labels.append(lab)
    return labels


def _read_run(csv_path: Path, run_label: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "file_name" not in df.columns:
        raise ValueError(f"{csv_path} no tiene columna 'file_name'")
    df.insert(0, "run_label", run_label)
    return df

def _infer_labels(csv_paths: List[Path], labels: Optional[List[str]]) -> List[str]:
    if labels:
        if len(labels) != len(csv_paths):
            raise ValueError("--labels debe tener la misma cantidad que los CSVs")
        return labels
    # por defecto, usar stem del archivo
    return [p.stem for p in csv_paths]

def _ensure_metrics_order(cols: List[str]) -> List[str]:
    # mantener el orden indicado y luego agregar cualquier columna extra al final
    base = [c for c in DEFAULT_METRICS_ORDER if c in cols]
    extras = [c for c in cols if c not in base and c not in ("file_name","run_label")]
    return ["file_name","run_label"] + base + extras

def build_wide_table(df_all: pd.DataFrame, metrics: List[str]) -> pd.DataFrame:
    """
    Retorna un DF con índice = file_name y columnas MultiIndex (run_label, metric).
    """
    keep_cols = ["file_name", "run_label"] + metrics
    df = df_all[keep_cols].copy()
    # pivot a columnas multi-index
    wide = (df
            .set_index(["file_name", "run_label"])
            .unstack("run_label"))
    # ordenar niveles: primero run_label, luego metric
    wide = wide.sort_index(axis=1, level=[0,1])
    return wide

def compute_deltas(wide: pd.DataFrame, baseline_label: str) -> pd.DataFrame:
    """
    Calcula deltas (corrida - baseline) para cada métrica, por archivo.
    Asegura que iteramos sobre una *copia congelada* de labels para no
    incluir columnas nuevas (Δ ...) durante el loop.
    """
    # Validación de MultiIndex
    if wide.columns.nlevels != 2:
        raise ValueError("Se esperaban columnas multi-índice (metric, run_label)")

    # Detectar cuál nivel es métricas y cuál run_label
    level0 = list(wide.columns.levels[0])
    level1 = list(wide.columns.levels[1])

    # Si baseline está en level0, significa que el orden es (run_label, metric) -> swapeamos
    if baseline_label in level0 and baseline_label not in level1:
        wide = wide.swaplevel(0, 1, axis=1).sort_index(axis=1)

    # Ahora asumimos (metric, run_label)
    if baseline_label not in wide.columns.get_level_values(1):
        raise ValueError(f"Baseline '{baseline_label}' no está en las columnas")

    # Congelar listas de labels ANTES de agregar columnas Δ
    metric_labels = list(pd.Index(wide.columns.get_level_values(0)).unique())
    # filtramos pseudo-métricas si estuvieran
    metric_labels = [m for m in metric_labels if m not in ("file_name", "run_label")]
    run_labels = list(pd.Index(wide.columns.get_level_values(1)).unique())
    run_labels_no_base = [r for r in run_labels if r != baseline_label]

    # Guardar pares (colname_tuple, serie) y asignar al final
    new_cols: dict[tuple, pd.Series] = {}

    for metric in metric_labels:
        # puede faltar el baseline para alguna métrica si hubo NaNs o merges raros
        if (metric, baseline_label) not in wide.columns:
            continue
        base_series = wide[(metric, baseline_label)]
        for run in run_labels_no_base:
            # si no existe esta métrica en ese run, saltamos
            if (metric, run) not in wide.columns:
                continue
            delta_col = (f"Δ {metric}", f"{run} - {baseline_label}")
            new_cols[delta_col] = wide[(metric, run)] - base_series

    # Asignar todas las columnas Δ de una
    for col, series in new_cols.items():
        wide[col] = series

    # Ordenar columnas por índice
    wide = wide.sort_index(axis=1)
    return wide


def summarize_by_run(df_all: pd.DataFrame, metrics: List[str]) -> pd.DataFrame:
    """
    Devuelve agregados por corrida: count, sum, mean, median para cada métrica.
    """
    grp = df_all.groupby("run_label")[metrics]
    out_sum = grp.sum().add_prefix("sum_")
    out_mean = grp.mean(numeric_only=True).add_prefix("mean_")
    out_med = grp.median(numeric_only=True).add_prefix("median_")
    out_cnt = grp.count().add_prefix("count_")
    summary = pd.concat([out_cnt, out_sum, out_mean, out_med], axis=1)
    return summary.reset_index()

def export_top_changes(
    deltas_only: pd.DataFrame,
    metric: str,
    baseline_label: str,
    out_dir: Path,
    topn: int = 20
) -> pd.DataFrame:
    """
    Extrae las top variaciones absolutas por 'metric' y las guarda en CSV.
    """
    # columnas candidatas tipo ("Δ metric", "runX - baseline")
    target_prefix = f"Δ {metric}"
    candidates = [c for c in deltas_only.columns if isinstance(c, tuple) and c[0] == target_prefix]
    if not candidates:
        return pd.DataFrame()

    # Para seleccionar top por cada delta-col, concatenamos rankings
    parts = []
    for col in candidates:
        tmp = deltas_only[col].abs().sort_values(ascending=False).head(topn)
        part = pd.DataFrame({
            "file_name": tmp.index,
            "delta_abs": tmp.values,
            "delta": deltas_only[col].loc[tmp.index].values,
            "comparison": col[1],
            "metric": metric,
        })
        parts.append(part)

    top = pd.concat(parts, ignore_index=True)
    out_path = out_dir / f"top_changes_{metric.replace('/', '_')}.csv"
    top.to_csv(out_path, index=False)
    return top

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Compara múltiples CSVs de debug y calcula deltas por archivo y métrica."
    )
    ap.add_argument("csvs", nargs="+", help="Rutas a 2+ CSVs de debug")
    ap.add_argument("--labels", nargs="*", default=None, help="Etiquetas por CSV (mismo largo que csvs)")
    ap.add_argument("--baseline", default=None, help="Etiqueta de baseline (default: primera corrida)")
    ap.add_argument("--out-dir", default="troubleshooting/out", help="Directorio de salida")
    ap.add_argument("--inner", action="store_true", help="Usar merge inner (default: outer) por file_name")
    ap.add_argument("--topn", type=int, default=20, help="Cuántos archivos mostrar por métrica en top changes")
    ap.add_argument("--metrics", nargs="*", default=None,
                    help="Filtrar a este set de métricas (default: auto detect + orden recomendado)")
    ap.add_argument("--pattern", default="*.csv",
                    help="Patrón de archivos dentro de carpetas (default: *.csv)")
    ap.add_argument("--use-parent-label", action="store_true",
                    help="Nombrar cada corrida con el nombre de la carpeta contenedora (ignora --labels)")
    return ap.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # 1) Expandir entradas: rutas a archivos, globs y carpetas
    csv_paths = _expand_csv_args(args.csvs, args.pattern)
    if len(csv_paths) < 2:
        print("Necesitás al menos 2 CSVs/corridas para comparar.", file=sys.stderr)
        return 2

    # 2) Etiquetas
    if args.use_parent_label:
        labels = _labels_from_paths(csv_paths, use_parent=True)
    else:
        labels = _infer_labels(csv_paths, args.labels)

    # 3) Baseline
    baseline_label = labels[0] if args.baseline is None else args.baseline

    # 4) Verificaciones de existencia
    for p in csv_paths:
        if not p.exists():
            print(f"No existe: {p}", file=sys.stderr)
            return 2

    # 5) Leer corridas
    dfs = []
    for p, lab in zip(csv_paths, labels):
        df = _read_run(p, lab)
        dfs.append(df)

    # 6) Unir y preparar set de métricas
    df_all = pd.concat(dfs, ignore_index=True, sort=False)
    metric_candidates = [c for c in df_all.columns if c not in ("file_name", "run_label")]
    metrics = args.metrics if args.metrics else _ensure_metrics_order(metric_candidates)[2:]

    # Reordenar columnas
    df_all = df_all[["file_name", "run_label"] + metrics].copy()

    # 7) Merge "inner" opcional (quedarse solo con archivos presentes en todas las corridas)
    if args.inner:
        present = (df_all.groupby(["file_name", "run_label"]).size()
                   .reset_index()[["file_name", "run_label"]])
        counts = present.groupby("file_name").size()
        keep_files = counts[counts == len(labels)].index
        df_all = df_all[df_all["file_name"].isin(keep_files)]

    # 8) Salida
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 9) Armar wide y guardar
    wide = build_wide_table(df_all, metrics)
    (out_dir / "combined_wide.csv").write_text("", encoding="utf-8")  # asegurar archivo si no hay permisos? opcional
    wide.to_csv(out_dir / "combined_wide.csv", index=True)

    # 10) Deltas vs baseline
    try:
        wide_with_deltas = compute_deltas(wide.copy(), baseline_label)
    except ValueError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        print(f"Labels disponibles: {sorted(set(wide.columns.get_level_values(1)))}", file=sys.stderr)
        return 2

    delta_cols = [c for c in wide_with_deltas.columns if isinstance(c, tuple) and c[0].startswith("Δ ")]
    deltas_only = wide_with_deltas[delta_cols].copy()
    deltas_only.to_csv(out_dir / f"deltas_vs_{baseline_label}.csv", index=True)

    # 11) Resumen por corrida
    summary = summarize_by_run(df_all, metrics)
    summary.to_csv(out_dir / "summary_by_run.csv", index=False)

    # 12) Tops por métrica
    for m in metrics:
        export_top_changes(deltas_only, m, baseline_label, out_dir, topn=args.topn)

    # 13) Consola: preview
    print("\n=== Runs detectados ===")
    for p, lab in zip(csv_paths, labels):
        print(f"- {lab}: {p}")

    print(f"\nBaseline: {baseline_label}")
    print(f"Archivos procesados (unique file_name): {wide.index.size}")

    print("\nEjemplo (primeros 5 deltas):")
    try:
        print(deltas_only.head(5))
    except Exception:
        print("(No hay columnas Δ para previsualizar)")

    print(f"\nSalidas en: {out_dir.resolve()}")
    print(" - combined_wide.csv")
    print(f" - deltas_vs_{baseline_label}.csv")
    print(" - summary_by_run.csv")
    print(" - top_changes_<metric>.csv (uno por métrica)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
