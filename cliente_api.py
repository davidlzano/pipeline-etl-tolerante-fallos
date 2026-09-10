"""
Cliente de la API con tolerancia a fallos.

El pipeline corre desatendido a las 6 de la mañana. Nadie lo está mirando, así
que la diferencia entre un proceso que sirve y uno que no está en qué hace
cuando algo sale mal, no en qué hace cuando todo sale bien.

Tres decisiones gobiernan este módulo:

1. No todos los errores merecen reintento. Un 503 es el servidor ocupado y se
   resuelve esperando; un 404 es el endpoint equivocado y esperar no lo va a
   arreglar. Reintentar un 404 tres veces solo retrasa el aviso del problema
   real.

2. La espera crece entre intentos. Si el servidor está saturado, golpearlo
   cada segundo empeora la situación.

3. Cuando la respuesta no es JSON, el cuerpo se guarda a disco. Un mensaje
   que dice "error de parseo" no permite diagnosticar nada; el HTML del portal
   de autenticación que devolvió el proxy, sí.
"""

import time
from pathlib import Path

from api_simulada import ErrorAPI, TimeoutAPI, consultar

DIR_ERRORES = Path("errores_api")

# Códigos que se corrigen solos con el tiempo. Todo lo demás aborta.
REINTENTABLES = {408, 429, 500, 502, 503, 504}

MAX_CUERPO_EN_CONSOLA = 300


def _vale_la_pena_reintentar(status_code: int) -> bool:
    return status_code in REINTENTABLES


def _resumir(texto: str, limite: int = MAX_CUERPO_EN_CONSOLA) -> str:
    texto = " ".join((texto or "").split())
    return texto if len(texto) <= limite else texto[:limite] + "..."


def _guardar_cuerpo_error(cuerpo: str, status_code: int) -> Path | None:
    """Persiste la respuesta completa para diagnóstico posterior."""
    if not cuerpo:
        return None
    DIR_ERRORES.mkdir(exist_ok=True)
    marca = time.strftime("%Y%m%d_%H%M%S")
    ruta = DIR_ERRORES / f"error_{status_code}_{marca}.html"
    ruta.write_text(cuerpo, encoding="utf-8")
    return ruta


def consultar_api(
    payload: dict,
    intentos: int = 3,
    espera_seg: int = 2,
    aceptar_vacio: bool = True,
) -> list[dict] | None:
    """
    Consulta la API con reintentos.

    aceptar_vacio=False trata una respuesta vacía como fallo y reintenta. Se
    usa cuando el rango consultado siempre debería traer datos: un resultado
    vacío ahí significa que algo se rompió, no que no hubo movimiento.

    Devuelve los registros, o None si agotó los intentos.

    En producción la espera base son 30 segundos; aquí se usan 2 para que la
    demostración no tarde minutos.
    """
    for intento in range(1, intentos + 1):
        try:
            registros = consultar(payload)

            if not registros and not aceptar_vacio:
                print("   Respuesta vacia donde se esperaban datos")
            else:
                return registros

        except TimeoutAPI as e:
            print(f"   Timeout: {e}")

        except ErrorAPI as e:
            if e.cuerpo and "<html" in e.cuerpo.lower():
                print(f"   HTTP {e.status_code}: la respuesta no es JSON valido")
                print(f"      Cuerpo: {_resumir(e.cuerpo)}")
                ruta = _guardar_cuerpo_error(e.cuerpo, e.status_code)
                if ruta:
                    print(f"      Respuesta completa guardada en {ruta}")
            else:
                print(f"   HTTP {e.status_code}: {e}")

            if not _vale_la_pena_reintentar(e.status_code):
                print(f"   HTTP {e.status_code} no se corrige reintentando; se aborta.")
                return None

        if intento < intentos:
            espera = espera_seg * intento  # 2s, 4s, 6s...
            print(f"   Reintentando en {espera}s (intento {intento}/{intentos})")
            time.sleep(espera)

    print(f"   Agotados los {intentos} intentos")
    return None
