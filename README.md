# Pipeline ETL con tolerancia a fallos y control de cambios

Integración diaria desde la API de un sistema transaccional hacia un data warehouse. Corre desatendido, se recupera solo de los fallos de la fuente y carga únicamente lo que cambió.

Este repositorio es una **reimplementación demostrativa** de un proceso que diseñé y mantengo en producción, donde integra diez tablas de órdenes de producción y costos y accede a un nivel de detalle que las exportaciones manuales del sistema no entregan. El código aquí publicado es original, corre contra una API simulada y no contiene información de la empresa.

---

## El problema

Un proceso programado a las 6 de la mañana tiene una característica incómoda: nadie lo está mirando. Lo que distingue un pipeline que sirve de uno que no es qué hace cuando algo falla, no qué hace cuando todo sale bien.

Tres cosas fallan de forma rutinaria:

- **La API no responde.** Timeouts, servidor saturado, mantenimiento.
- **La API responde algo que no es JSON.** Un proxy de autenticación devuelve HTML con HTTP 200 y el parser revienta.
- **El proceso no corre.** El servidor se reinició, la tarea programada no se disparó.

Y una cuarta cosa, más silenciosa: recargar la misma ventana de datos todos los días duplica registros o destruye la marca de auditoría de cuándo entró cada uno.

## El enfoque

### Ventana móvil auto-recuperable

Cada corrida consulta los últimos 90 días, no solo el día anterior. Si la API falla un martes, el miércoles el rango vuelve a cubrir el martes sin que nadie reprocese nada a mano.

Esto es lo que convierte un proceso frágil en uno que tolera días completos caídos. El costo es traer datos que ya se tienen, y ese costo se resuelve con el control de cambios.

### Control de cambios por hash de fila

Cada registro se reduce a un hash MD5 de su contenido. Si el hash ya existe en destino, la fila no cambió y se descarta.

Lo delicado no es el hash sino la **normalización previa**. La misma orden puede llegar hoy con cantidad `94.00` y mañana con `94`, o con el cliente en minúsculas. Sin canonizar los valores, cada variación de formato se vería como un cambio real y el control de cambios no serviría de nada.

Las reglas de normalización:

| Entrada | Se convierte en |
|---|---|
| `None`, `NaN`, `""`, `0` | omitido del hash |
| `"94.00"`, `94.0`, `94` | `"94"` |
| `" propalcote "` | `"PROPALCOTE"` |
| fechas | `"YYYY-MM-DD HH:MM:SS"` |

Omitir los vacíos tiene una consecuencia que conviene entender: **agregar una columna nueva a la tabla no altera los hashes históricos.** Los registros viejos la reciben en NULL, NULL se omite, y su hash queda idéntico. Sin esa regla, cualquier cambio de esquema marcaría la tabla entera como modificada y forzaría una recarga completa.

### Errores reintentables y errores que no lo son

Un 503 es el servidor ocupado: se resuelve esperando. Un 404 es el endpoint equivocado: esperar no lo va a arreglar. Reintentar un 404 tres veces solo retrasa el aviso del problema real.

```
408, 429, 500, 502, 503, 504  ->  reintenta con espera creciente
cualquier otro                ->  aborta y reporta
```

La espera crece entre intentos. Si el servidor está saturado, golpearlo cada segundo empeora la situación.

### Respuestas no-JSON guardadas a disco

Cuando la respuesta no se puede deserializar, el cuerpo completo se persiste en `errores_api/`. Un mensaje que dice "error de parseo" no permite diagnosticar nada; el HTML del portal de autenticación que devolvió el proxy, sí.

### Aplanado de estructuras anidadas

La API entrega cada orden con su detalle adentro. Un warehouse necesita eso en dos tablas relacionadas, no en una columna con JSON. El pipeline separa el detalle a su propia tabla conservando la clave del padre.

### Atributos derivados desde texto libre

El sistema origen guarda el sustrato como una cadena: `PROPALCOTE 240 G C12 x 70CM`. Analizar por gramaje o por calibre exige convertir eso en columnas, y ese trabajo se hace una vez en el pipeline en lugar de repetirse en cada consulta.

---

## Ejecución

Requiere Python 3.10 o superior.

```bash
pip install -r requirements.txt

python pipeline.py --reset   # primera corrida, warehouse vacío
python pipeline.py           # segunda corrida
```

La API simulada **falla a propósito el 35% de las veces**, de modo que la tolerancia a fallos se puede ver operando en lugar de solo leerse aquí. Ejecuta varias veces para ver los distintos modos: timeout, error transitorio con reintento, 404 que aborta, y respuesta HTML guardada a disco.

Primera corrida:

```
Consultando API...
   HTTP 503: Error transitorio del servidor (503)
   Reintentando en 2s (intento 1/3)
   179 ordenes recibidas

Tabla                     Nuevas   Sin cambios     Total
--------------------------------------------------------
ordenes_produccion           179             0       179
detalle_ordenes              442             0       442
```

Segunda corrida, sobre los mismos 90 días:

```
Tabla                     Nuevas   Sin cambios     Total
--------------------------------------------------------
ordenes_produccion            28           151       207
detalle_ordenes                0           442       442
```

151 órdenes se reconocieron como idénticas y se descartaron. Las 28 nuevas son las que cambiaron de estado entre corridas — exactamente lo que el control de cambios debe detectar. El detalle no cambió, así que no se escribió nada.

---

## Estructura

| Archivo | Contenido |
|---|---|
| `api_simulada.py` | API con fallos controlados: timeout, 5xx, 404, HTML |
| `cliente_api.py` | Reintentos, espera creciente, clasificación de errores |
| `transformaciones.py` | Normalización, hash de fila, aplanado de anidados |
| `pipeline.py` | Orquestación, ventana móvil, carga incremental |

---

## Diferencias con la versión en producción

| | Aquí | Producción |
|---|---|---|
| Fuente | API simulada local | API REST del ERP |
| Destino | SQLite | SQL Server |
| Tablas | 2 | 10 |
| Espera entre reintentos | 2s | 30s |
| Orquestación | manual | Programador de tareas de Windows |
| Notificación | consola | correo HTML con semáforo de estado |

El algoritmo de normalización, el cálculo de hash y la clasificación de errores son los mismos.

---

## Posibles extensiones

- Reporte por correo al terminar, con estado y detalle de la corrida
- Registro de la ejecución en una tabla de control, para medir tendencias de fallo
- Detección de eliminaciones en origen, hoy fuera de alcance

---

## Licencia

MIT
