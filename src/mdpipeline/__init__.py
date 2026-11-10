"""Pipeline de market data de baja latencia.

Tres piezas, y cada una responde a una critica concreta del disenio original:

- :mod:`mdpipeline.latency` **percentiles, nunca promedios**, y la separacion
  entre medir latencia y medir throughput, que no se pueden obtener de la misma
  corrida.
- :mod:`mdpipeline.orderbook` reconstruccion del libro L2 con **deteccion de
  huecos de secuencia** y recuperacion por snapshot. Seguir aplicando deltas tras
  un hueco produce precios erroneos sin ninguna senial.
- ``mdpipeline._spsc`` ring buffer **SPSC wait-free** en C++20, con relleno a
  linea de cache y ordenamiento de memoria acquire/release explicito.

El nucleo nativo no se opera desde Python para medir rendimiento: cruzar la
frontera de pybind11 cuesta cientos de nanosegundos, tres ordenes de magnitud
mas que la operacion que se quiere medir. El productor y el consumidor corren
enteros en C++ con el GIL liberado.
"""

from __future__ import annotations

from . import _spsc
from .latency import (
    LatencyProfile,
    ThroughputResult,
    clock_resolution_ns,
    measure_latency,
    measure_throughput,
)
from .orderbook import BookState, GapDetected, Level, OrderBook, Side

__version__ = "0.1.0"

RingBuffer = _spsc.RingBuffer
"""Ring buffer SPSC wait-free. Para verificar correccion, no rendimiento."""

CACHE_LINE_SIZE = int(_spsc.__cache_line_size__)
TICK_SIZE_BYTES = int(_spsc.__tick_size__)

__all__ = [
    "CACHE_LINE_SIZE",
    "TICK_SIZE_BYTES",
    "BookState",
    "GapDetected",
    "LatencyProfile",
    "Level",
    "OrderBook",
    "RingBuffer",
    "Side",
    "ThroughputResult",
    "__version__",
    "clock_resolution_ns",
    "measure_latency",
    "measure_throughput",
]
