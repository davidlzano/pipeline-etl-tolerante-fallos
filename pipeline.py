"""
Pipeline de integración diaria.

Trae las órdenes de producción de una ventana móvil de 90 días desde la API,
las normaliza, separa el detalle anidado en su propia tabla y las carga en el
warehouse cargando únicamente lo que cambió.

Uso:
    python pipeline.py           # corrida normal
    python pipeline.py --reset   # borra el warehouse y arranca de cero
"""

import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from cliente_api import consultar_api
from transformaciones import (
    agregar_auditoria,
    agregar_id_fila,
    extraer_subtabla,
)

RUTA_DB = Path("warehouse.db")

# Ventana móvil. Es la decisión que hace el proceso auto-recuperable: si la
# API falla un día, al siguiente el rango vuelve a cubrir los días perdidos
# sin que nadie tenga que reprocesar nada a mano.
DIAS_VENTANA = 90

TABLA_ORDENES = "ordenes_produccion"
TABLA_DETALLE = "detalle_ordenes"


def crear_esquema(con: sqlite3.Connection) -> None:
    con.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLA_ORDENES} (
            op          TEXT,
            fecha       TEXT,
            cliente     TEXT,
            estado      TEXT,
            referencia  TEXT,
            cantidad    INTEGER,
            gramaje     INTEGER,
            calibre     INTEGER,
            ancho_cm    INTEGER,
            id_fila     TEXT,
            fecha_carga TEXT
        );
        CREATE TABLE IF NOT EXISTS {TABLA_DETALLE} (
            op             TEXT,
            item           INTEGER,
            descripcion    TEXT,
            unidades       INTEGER,
            costo_unitario REAL,
            id_fila        TEXT,
            fecha_carga    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ord_hash ON {TABLA_ORDENES}(id_fila);
        CREATE INDEX IF NOT EXISTS idx_det_hash ON {TABLA_DETALLE}(id_fila);
        CREATE INDEX IF NOT EXISTS idx_ord_op ON {TABLA_ORDENES}(op);
        """
    )
    con.commit()


# =============================================================================
# EXTRACCIÓN DE ATRIBUTOS DESDE TEXTO
# =============================================================================

def _extraer_gramaje(referencia: str) -> int | None:
    """Gramaje en g/m2, del patrón '240 G' dentro de la referencia."""
    partes = str(referencia).upper().split()
    for i, p in enumerate(partes):
        if p == "G" and i > 0 and partes[i - 1].isdigit():
            return int(partes[i - 1])
    return None


def _extraer_calibre(referencia: str) -> int | None:
    """Calibre, del patrón 'C12'."""
    for p in str(referencia).upper().split():
        if p.startswith("C") and p[1:].isdigit():
            return int(p[1:])
    return None


def _extraer_ancho(referencia: str) -> int | None:
    """Ancho en centímetros, del patrón '70CM'."""
    for p in str(referencia).upper().replace("X", " ").split():
        if p.endswith("CM") and p[:-2].isdigit():
            return int(p[:-2])
    return None


def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deriva atributos estructurados desde el texto libre de la referencia.

    El sistema origen guarda el sustrato como una cadena. Analizar por gramaje
    o por calibre exige convertir esa cadena en columnas, y ese trabajo se hace
    una vez en el pipeline en lugar de repetirse en cada consulta.
    """
    df["gramaje"] = df["referencia"].map(_extraer_gramaje)
    df["calibre"] = df["referencia"].map(_extraer_calibre)
    df["ancho_cm"] = df["referencia"].map(_extraer_ancho)
    return df


# =============================================================================
# CARGA CON CONTROL DE CAMBIOS
# =============================================================================

def leer_hashes_existentes(con: sqlite3.Connection, tabla: str, claves: list[str]) -> set[str]:
    """
    Hashes ya presentes para las claves dadas.

    Se consulta en lotes porque los motores de base de datos limitan el número
    de parámetros por consulta. Con noventa días de órdenes se supera ese
    límite sin dificultad.
    """
    hashes: set[str] = set()
    LOTE = 500
    for i in range(0, len(claves), LOTE):
        lote = claves[i : i + LOTE]
        marcadores = ",".join("?" * len(lote))
        filas = con.execute(
            f"SELECT DISTINCT id_fila FROM {tabla} "
            f"WHERE op IN ({marcadores}) AND id_fila IS NOT NULL",
            lote,
        ).fetchall()
        hashes.update(f[0].strip().lower() for f in filas if f[0])
    return hashes


def cargar_incremental(con: sqlite3.Connection, df: pd.DataFrame, tabla: str) -> dict:
    """
    Escribe solo las filas cuyo hash no exista en destino.

    Devuelve el conteo de filas nuevas y de filas sin cambios, que es lo que
    alimenta el reporte de la corrida.
    """
    if df.empty:
        return {"nuevas": 0, "sin_cambios": 0}

    claves = df["op"].astype(str).unique().tolist()
    existentes = leer_hashes_existentes(con, tabla, claves)

    nuevas = df[~df["id_fila"].str.lower().isin(existentes)].copy()
    sin_cambios = len(df) - len(nuevas)

    if not nuevas.empty:
        nuevas.to_sql(tabla, con, if_exists="append", index=False)

    return {"nuevas": len(nuevas), "sin_cambios": sin_cambios}


# =============================================================================
# ORQUESTACIÓN
# =============================================================================

def ejecutar() -> int:
    hasta = date.today()
    desde = hasta - timedelta(days=DIAS_VENTANA)

    print("=" * 62)
    print(f"Pipeline de ordenes de produccion")
    print(f"Ventana movil: {desde} a {hasta} ({DIAS_VENTANA} dias)")
    print("=" * 62)

    print("\nConsultando API...")
    registros = consultar_api(
        {"desde": desde.isoformat(), "hasta": hasta.isoformat()},
        aceptar_vacio=False,
    )

    if registros is None:
        print("\nLa API no respondio. El proceso termina sin cargar.")
        print("La ventana movil cubrira estos dias en la proxima corrida.")
        return 1

    print(f"   {len(registros)} ordenes recibidas")

    # --- Tabla principal ---
    df_ordenes = pd.DataFrame(
        [{k: v for k, v in r.items() if k != "detalle"} for r in registros]
    )
    df_ordenes = enriquecer(df_ordenes)
    df_ordenes = agregar_id_fila(df_ordenes)
    df_ordenes = agregar_auditoria(df_ordenes)

    # --- Detalle anidado a su propia tabla ---
    df_detalle = extraer_subtabla(registros, "detalle", ["op"])
    if not df_detalle.empty:
        df_detalle = agregar_id_fila(df_detalle)
        df_detalle = agregar_auditoria(df_detalle)

    con = sqlite3.connect(RUTA_DB)
    try:
        crear_esquema(con)
        print("\nCargando con control de cambios...")
        r_ord = cargar_incremental(con, df_ordenes, TABLA_ORDENES)
        r_det = cargar_incremental(con, df_detalle, TABLA_DETALLE)
        con.commit()

        total_ord = con.execute(f"SELECT COUNT(*) FROM {TABLA_ORDENES}").fetchone()[0]
        total_det = con.execute(f"SELECT COUNT(*) FROM {TABLA_DETALLE}").fetchone()[0]
    finally:
        con.close()

    print(f"\n{'Tabla':<22}{'Nuevas':>10}{'Sin cambios':>14}{'Total':>10}")
    print("-" * 56)
    print(f"{TABLA_ORDENES:<22}{r_ord['nuevas']:>10}{r_ord['sin_cambios']:>14}{total_ord:>10}")
    print(f"{TABLA_DETALLE:<22}{r_det['nuevas']:>10}{r_det['sin_cambios']:>14}{total_det:>10}")

    print(f"\nWarehouse: {RUTA_DB.resolve()}")
    print("\nVuelve a ejecutar el pipeline: la segunda corrida deberia cargar")
    print("muy pocas filas, porque el resto ya esta en destino sin cambios.")
    return 0


def main() -> None:
    if "--reset" in sys.argv and RUTA_DB.exists():
        RUTA_DB.unlink()
        print(f"Warehouse eliminado: {RUTA_DB}\n")
    raise SystemExit(ejecutar())


if __name__ == "__main__":
    main()
