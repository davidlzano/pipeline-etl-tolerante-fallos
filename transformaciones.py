"""
Normalización de valores y cálculo del hash de fila.

El pipeline recarga una ventana móvil de 90 días todos los días. Sin control
de cambios eso significa reescribir noventa días de datos cada mañana: lento,
y destruye cualquier marca de auditoría sobre cuándo entró realmente cada
registro.

La solución es un hash del contenido de cada fila. Si el hash ya existe en
destino, la fila no cambió y se descarta. Solo se escribe lo nuevo y lo
modificado.

Lo delicado no es el hash sino la normalización previa. La misma orden puede
llegar hoy con cantidad 94.00 y mañana con 94, o con el cliente en minúsculas.
Sin canonizar, cada variación de formato se vería como un cambio real y el
control de cambios no serviría de nada.

Reglas:
    - Nulos, vacíos y ceros numéricos se omiten del hash
    - Números se unifican: "94.00", "94.0" y 94 son todos "94"
    - Texto sin espacios al borde y en mayúsculas
    - Fechas en formato fijo
    - Columnas ordenadas alfabéticamente

Omitir los vacíos tiene una consecuencia que vale la pena entender: agregar
una columna nueva a la tabla, que llega en NULL para los registros viejos, no
altera sus hashes históricos. Sin esa regla, cualquier cambio de esquema
marcaría toda la tabla como modificada.
"""

import hashlib
from datetime import date, datetime

import pandas as pd

# Columnas de control que no describen el contenido del registro
COLS_EXCLUIDAS = {"fecha_carga", "id_fila"}


def normalizar_valor(v) -> str:
    """Forma canónica de un valor. Devuelve "" si debe omitirse del hash."""
    if v is None:
        return ""

    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass

    if isinstance(v, (pd.Timestamp, datetime, date)):
        return pd.Timestamp(v).strftime("%Y-%m-%d %H:%M:%S")

    s = str(v).strip().upper()
    if s in ("", "NONE", "NAN", "NAT"):
        return ""

    try:
        f = float(s)
        if f == 0:
            return ""
        return str(int(f)) if f == int(f) else repr(f)
    except (ValueError, OverflowError):
        return s


def calcular_id_fila(row, columnas) -> str:
    """MD5 del contenido de la fila, sobre las columnas ordenadas."""
    partes = []
    for col in sorted(c for c in columnas if c.lower() not in COLS_EXCLUIDAS):
        val = normalizar_valor(row[col])
        if val != "":
            partes.append(f"{col.lower()}={val}")
    return hashlib.md5("|".join(partes).encode("utf-8")).hexdigest()


def agregar_id_fila(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in df.columns if c.lower() not in COLS_EXCLUIDAS]
    df["id_fila"] = df.apply(lambda row: calcular_id_fila(row, cols), axis=1)
    return df


def agregar_auditoria(df: pd.DataFrame) -> pd.DataFrame:
    df["fecha_carga"] = pd.Timestamp.now()
    return df


def extraer_subtabla(registros: list[dict], clave: str, campos_padre: list[str]) -> pd.DataFrame:
    """
    Aplana una lista anidada del JSON a su propia tabla.

    La API devuelve cada orden con su detalle adentro. Un data warehouse
    necesita eso en dos tablas relacionadas, no en una columna con JSON.
    """
    filas = []
    for reg in registros:
        hijos = reg.get(clave) or []
        for hijo in hijos:
            fila = {campo: reg.get(campo) for campo in campos_padre}
            fila.update(hijo)
            filas.append(fila)
    return pd.DataFrame(filas)
