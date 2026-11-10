"""Medicion de latencia en percentiles.

**La latencia nunca se reporta en promedio.** La distribucion de latencias tiene
cola muy pesada: el promedio lo domina el cuerpo de la distribucion y esconde
justo lo que importa. Un sistema con p50 de 100 ns y p99.9 de 50 microsegundos
tiene un promedio parecido al de uno con p50 de 150 ns y p99.9 de 300 ns, y son
sistemas completamente distintos.

Esta es la razon por la que las dos funciones de este modulo devuelven
percentiles y exponen el promedio solo marcado como lo que es: una cifra que no
debe usarse para decidir.

El segundo punto, igual de importante: **latencia y throughput no se miden en la
misma corrida**. Si el productor satura la cola, lo que se mide es espera en cola
--capacidad dividida por throughput-- y no el costo del traspaso. La funcion
:func:`measure_latency` paca al productor para mantener la cola superficial, y
:func:`measure_throughput` la satura a proposito.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import _spsc

__all__ = [
    "LatencyProfile",
    "ThroughputResult",
    "clock_resolution_ns",
    "measure_latency",
    "measure_throughput",
]


@dataclass(frozen=True)
class LatencyProfile:
    """Perfil de latencia en percentiles."""

    p50_ns: int
    p90_ns: int
    p99_ns: int
    p999_ns: int
    p9999_ns: int
    min_ns: int
    max_ns: int
    mean_ns: float
    """Promedio. **No usar para decidir**: se expone para poder mostrar cuanto
    difiere de los percentiles."""

    count: int
    max_queue_depth: int
    saturated: bool
    """``True`` si la cola llego a llenarse. Si lo es, estos numeros son espera en
    cola y **no** latencia de traspaso."""

    out_of_order: int
    capacity: int
    padded: bool
    clock_resolution_ns: int = 0
    """Resolucion efectiva del reloj durante la medicion."""

    @property
    def resolution_limited(self) -> bool:
        """``True`` si la mediana no supera un tic de reloj.

        Cuando lo es, el valor de ``p50_ns`` **no es el costo del traspaso**: es
        el tamanio del tic del reloj. El costo real esta en algun punto por
        debajo y este instrumento no puede resolverlo. Reportar la cifra como si
        fuera una medicion seria presentar un artefacto del instrumento.
        """
        return self.clock_resolution_ns > 0 and self.p50_ns <= self.clock_resolution_ns + 10

    @property
    def tail_ratio(self) -> float:
        """``p99.9 / p50``. Cuantifica el peso de la cola.

        Un valor cercano a 1 indica latencia predecible. Por encima de 10, la
        cola esta dominada por algo distinto del camino critico, tipicamente el
        planificador del sistema operativo.
        """
        return self.p999_ns / self.p50_ns if self.p50_ns else float("inf")

    @property
    def is_measurement_valid(self) -> bool:
        """``False`` si la cola se saturo, caso en que la medicion no es de latencia."""
        return not self.saturated

    def __str__(self) -> str:
        aviso = "" if self.is_measurement_valid else "  [SATURADA: esto es espera en cola]"
        if self.resolution_limited:
            aviso += f"  [p50 <= 1 tic de reloj de {self.clock_resolution_ns} ns]"
        return (
            f"p50 {self.p50_ns} ns | p90 {self.p90_ns} | p99 {self.p99_ns} | "
            f"p99.9 {self.p999_ns} | p99.99 {self.p9999_ns} | max {self.max_ns} "
            f"(cola p99.9/p50 = {self.tail_ratio:.1f}x){aviso}"
        )


@dataclass(frozen=True)
class ThroughputResult:
    """Resultado de una medicion de throughput."""

    messages_per_second: float
    n_messages: int
    elapsed_s: float
    capacity: int
    padded: bool
    queueing_latency_p50_ns: int
    """Latencia observada con la cola saturada: es espera en cola, no costo de
    traspaso. Se expone para poder comprobar la identidad con
    ``expected_queueing_latency_ns``."""

    saturated: bool
    """``True`` si la cola llego a su capacidad. La identidad de espera en cola
    solo se cumple con saturacion **sostenida**, que requiere suficientes
    mensajes para que el productor se adelante de forma estable."""

    max_queue_depth: int

    @property
    def expected_queueing_latency_ns(self) -> float:
        """``capacidad / throughput``, en nanosegundos.

        Es la espera en cola en estado estacionario con la cola llena. Coincide
        con ``queueing_latency_p50_ns`` **solo si la saturacion fue sostenida**:
        con pocos mensajes el consumidor sigue el ritmo durante buena parte de la
        corrida, la cola no se mantiene llena y la latencia observada queda por
        debajo. Comprobar ``saturated`` y ``max_queue_depth`` antes de leer la
        identidad.
        """
        if self.messages_per_second <= 0.0:
            return float("nan")
        return 1e9 * self.capacity / self.messages_per_second

    def __str__(self) -> str:
        return (
            f"{self.messages_per_second / 1e6:.1f}M msg/s "
            f"({self.n_messages:,} mensajes en {self.elapsed_s:.3f} s)"
        )


def clock_resolution_ns(n_samples: int = 2000) -> int:
    """Resolucion efectiva de ``steady_clock``, en nanosegundos.

    Es el dato que permite interpretar cualquier percentil de latencia. En
    Windows ``steady_clock`` resuelve alrededor de 100 ns; en Linux con
    ``CLOCK_MONOTONIC``, decenas. Si el traspaso del ring buffer cuesta menos
    que la resolucion, **este reloj no puede medirlo**: las lecturas de entrada y
    salida caen en el mismo tic.

    Returns
    -------
    int
    """
    if n_samples < 1:
        raise ValueError("n_samples debe ser positivo")
    return int(_spsc.clock_resolution_ns(n_samples))


def measure_latency(
    n_messages: int = 1_000_000,
    target_rate_per_sec: float = 500_000.0,
    capacity: int = 8192,
    padded: bool = True,
) -> LatencyProfile:
    """Mide la latencia de traspaso con la cola superficial.

    Parameters
    ----------
    target_rate_per_sec:
        Tasa a la que el productor envia. Tiene que estar **por debajo** de la
        capacidad de proceso del consumidor para que la cola no se sature; si se
        satura, el resultado lo indica con ``saturated`` y la medicion no es de
        latencia.
    padded:
        Relleno a linea de cache de los indices. Se puede desactivar para medir
        el costo del *false sharing*.

    Returns
    -------
    LatencyProfile
    """
    if target_rate_per_sec <= 0.0:
        raise ValueError(
            "para medir latencia hace falta una tasa objetivo positiva; con tasa cero "
            "el productor satura la cola y lo que se mide es espera en cola. Usar "
            "measure_throughput() para ese caso."
        )
    r = _spsc.benchmark(
        n_messages=n_messages,
        capacity=capacity,
        target_rate_per_sec=target_rate_per_sec,
        padded=padded,
        busy_wait=True,
    )
    return LatencyProfile(
        p50_ns=int(r["p50_ns"]),
        p90_ns=int(r["p90_ns"]),
        p99_ns=int(r["p99_ns"]),
        p999_ns=int(r["p999_ns"]),
        p9999_ns=int(r["p9999_ns"]),
        min_ns=int(r["min_ns"]),
        max_ns=int(r["max_ns"]),
        mean_ns=float(r["mean_ns"]),
        count=int(r["count"]),
        max_queue_depth=int(r["max_queue_depth"]),
        saturated=bool(r["saturated"]),
        out_of_order=int(r["out_of_order"]),
        capacity=int(r["capacity"]),
        padded=bool(r["padded"]),
        clock_resolution_ns=clock_resolution_ns(),
    )


def measure_throughput(
    n_messages: int = 3_000_000,
    capacity: int = 8192,
    padded: bool = True,
) -> ThroughputResult:
    """Mide el throughput maximo saturando la cola a proposito.

    Returns
    -------
    ThroughputResult
    """
    r = _spsc.benchmark(
        n_messages=n_messages,
        capacity=capacity,
        target_rate_per_sec=0.0,
        padded=padded,
        busy_wait=True,
    )
    return ThroughputResult(
        messages_per_second=float(r["throughput_msg_per_s"]),
        n_messages=n_messages,
        elapsed_s=float(r["elapsed_s"]),
        capacity=int(r["capacity"]),
        padded=bool(r["padded"]),
        queueing_latency_p50_ns=int(r["p50_ns"]),
        saturated=bool(r["saturated"]),
        max_queue_depth=int(r["max_queue_depth"]),
    )
