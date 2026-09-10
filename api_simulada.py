"""
API simulada de un sistema transaccional.

Reemplaza la API real contra la que corre el pipeline en producción. Genera
órdenes de producción con estructura anidada y, deliberadamente, falla parte
de las veces: así la tolerancia a fallos del cliente se puede ver operando en
lugar de solo leerse en la documentación.

Los modos de fallo replican los que aparecen en producción:

    timeout          la API no responde a tiempo
    500 / 502 / 503  error transitorio del servidor, reintentable
    404              endpoint equivocado, no se corrige reintentando
    html             respuesta con HTTP 200 pero cuerpo HTML en vez de JSON
                     (típico de un portal de autenticación intermedio)
"""

import random
from datetime import date, timedelta

# Probabilidad de que una llamada falle. Súbela para ver más reintentos.
TASA_FALLO = 0.35

SUSTRATOS = [
    "PROPALCOTE 240 G C12 x 70CM",
    "KRAFT 180 G C14 x 90CM",
    "BOND 90 G C8 x 65CM",
    "CARTULINA 300 G C16 x 100CM",
    "PROPALCOTE 150 G C10 x 72CM",
]
CLIENTES = ["ALFA S.A.", "BETA LTDA", "GAMMA S.A.S", "DELTA & CIA", "OMEGA GROUP"]
ESTADOS = ["ABIERTA", "EN PROCESO", "TERMINADA", "ANULADA"]


class ErrorAPI(Exception):
    """Fallo de la API con su código HTTP asociado."""

    def __init__(self, status_code: int, mensaje: str, cuerpo: str = ""):
        super().__init__(mensaje)
        self.status_code = status_code
        self.cuerpo = cuerpo


class TimeoutAPI(Exception):
    """La API no respondió dentro del tiempo límite."""


def _generar_orden(rnd: random.Random, numero: int, fecha: date) -> dict:
    """Una orden con su detalle anidado, como la entrega la API real."""
    sustrato = rnd.choice(SUSTRATOS)
    n_items = rnd.randint(1, 4)
    return {
        "op": f"OP-{numero:06d}",
        "fecha": fecha.isoformat(),
        "cliente": rnd.choice(CLIENTES),
        "estado": rnd.choice(ESTADOS),
        "referencia": sustrato,
        "cantidad": rnd.randint(500, 20000),
        "detalle": [
            {
                "item": i + 1,
                "descripcion": f"Proceso {rnd.choice(['CORTE', 'IMPRESION', 'TROQUEL', 'PEGUE'])}",
                "unidades": rnd.randint(100, 5000),
                "costo_unitario": round(rnd.uniform(10, 900), 2),
            }
            for i in range(n_items)
        ],
    }


def consultar(payload: dict, semilla: int | None = None) -> list[dict]:
    """
    Simula POST /api/v1/queries.

    Devuelve la lista de registros del rango pedido, o lanza una excepción
    que imita alguno de los fallos observados en producción.
    """
    rnd = random.Random(semilla)

    if rnd.random() < TASA_FALLO:
        modo = rnd.choice(["timeout", "500", "502", "503", "404", "html"])
        if modo == "timeout":
            raise TimeoutAPI("Sin respuesta dentro del tiempo limite")
        if modo == "404":
            raise ErrorAPI(404, "Not Found", "<html><body>404 Not Found</body></html>")
        if modo == "html":
            raise ErrorAPI(
                200,
                "La respuesta no es JSON valido",
                "<html><head><title>Sign in</title></head><body>...</body></html>",
            )
        raise ErrorAPI(int(modo), f"Error transitorio del servidor ({modo})")

    desde = date.fromisoformat(payload["desde"])
    hasta = date.fromisoformat(payload["hasta"])

    registros = []
    dia = desde
    numero_base = (desde.toordinal() % 10000) * 10
    while dia <= hasta:
        # Semilla derivada del día: el mismo día siempre devuelve los mismos
        # datos, salvo por las mutaciones controladas de abajo.
        rnd_dia = random.Random(dia.toordinal())
        for i in range(rnd_dia.randint(0, 4)):
            orden = _generar_orden(rnd_dia, numero_base + i, dia)
            registros.append(orden)
        dia += timedelta(days=1)

    # Una fracción de las órdenes cambia de estado entre corridas. Es lo que
    # el control de cambios por hash debe detectar y actualizar.
    for orden in registros:
        if random.random() < 0.10:
            orden["estado"] = random.choice(ESTADOS)

    return registros
