"""Demostracion del pipeline de market data.

Ejecutar con:

    python examples/pipeline_demo.py
"""

from __future__ import annotations

import statistics

from mdpipeline import (
    CACHE_LINE_SIZE,
    TICK_SIZE_BYTES,
    GapDetected,
    Level,
    OrderBook,
    Side,
    measure_latency,
    measure_throughput,
)
from mdpipeline import _spsc


def seccion(titulo: str) -> None:
    print(f"\n{'=' * 78}\n{titulo}\n{'=' * 78}")


def demo_latencia_vs_throughput() -> None:
    seccion("Latencia y throughput NO se miden en la misma corrida")
    print("Si el productor satura la cola, lo medido es espera en cola y no el")
    print("costo del traspaso. Es el error de medicion mas comun en este componente.\n")

    t = measure_throughput(n_messages=6_000_000, capacity=16_384)
    print("COLA SATURADA (mide throughput):")
    print(f"  throughput             : {t.messages_per_second / 1e6:.1f}M msg/s")
    print(f"  saturada               : {t.saturated}  (profundidad maxima {t.max_queue_depth:,})")
    print(f"  \"latencia\" p50         : {t.queueing_latency_p50_ns / 1000:.1f} us")
    print(f"  prevista capacidad/thr : {t.expected_queueing_latency_ns / 1000:.1f} us")
    print("  -> coinciden en orden de magnitud: es espera en cola, no traspaso\n")

    p = measure_latency(n_messages=1_000_000, target_rate_per_sec=500_000.0, capacity=8192)
    print("COLA SUPERFICIAL, tasa pautada a 500 000 msg/s (mide latencia):")
    print(f"  {p}")
    print(f"  profundidad maxima     : {p.max_queue_depth}  de {p.capacity} -> saturada: {p.saturated}")
    print(f"  mensajes fuera de orden: {p.out_of_order}")
    print(f"\n  La latencia de traspaso real ({p.p50_ns} ns) es "
          f"{t.queueing_latency_p50_ns // max(p.p50_ns, 1):,}x menor que la")
    print("  espera en cola saturada. Confundirlas invalida cualquier conclusion.")


def demo_promedios() -> None:
    seccion("Por que la latencia no se reporta en promedio")
    p = measure_latency(n_messages=1_000_000, target_rate_per_sec=400_000.0)
    print(f"  mediana (p50) : {p.p50_ns:>10,} ns")
    print(f"  promedio      : {p.mean_ns:>10,.0f} ns")
    print(f"  p99           : {p.p99_ns:>10,} ns")
    print(f"  p99.9         : {p.p999_ns:>10,} ns")
    print(f"  p99.99        : {p.p9999_ns:>10,} ns")
    print(f"  maximo        : {p.max_ns:>10,} ns")
    print(f"\n  razon p99.9 / p50 = {p.tail_ratio:.0f}x")
    print("\n  La distribucion tiene cola muy pesada. Dos sistemas con el mismo")
    print("  promedio pueden tener colas que difieren en ordenes de magnitud, y es")
    print("  la cola la que determina si el sistema sirve.")
    print("\n  Advertencia honesta: en una maquina sin nucleos aislados ni kernel")
    print("  ajustado, la cola por encima de p99.9 esta dominada por el planificador")
    print("  del sistema operativo, no por la cola. Para medir el componente hay que")
    print("  fijar afinidad de nucleo y aislarlos del planificador.")


def demo_false_sharing() -> None:
    seccion("Relleno a linea de cache: el costo del false sharing, medido")
    print(f"linea de cache: {CACHE_LINE_SIZE} bytes   tick: {TICK_SIZE_BYTES} bytes\n")
    print("Si los indices de lectura y escritura caen en la misma linea de cache,")
    print("cada escritura de un hilo invalida la linea en el nucleo del otro. Los")
    print("dos hilos no comparten datos, pero el protocolo de coherencia actua como")
    print("si lo hicieran.\n")
    print(f"{'capacidad':>10} {'con relleno':>14} {'sin relleno':>14} {'ganancia':>10}")
    for cap in (1024, 4096, 16_384):
        medianas = {}
        for padded in (True, False):
            muestras = [
                _spsc.benchmark(
                    n_messages=3_000_000, capacity=cap, target_rate_per_sec=0.0, padded=padded
                )["throughput_msg_per_s"]
                / 1e6
                for _ in range(3)
            ]
            medianas[padded] = statistics.median(muestras)
        ganancia = medianas[True] / medianas[False] - 1.0
        print(f"{cap:>10,} {medianas[True]:>12.1f}M {medianas[False]:>12.1f}M {ganancia:>+9.1%}")


def demo_libro() -> None:
    seccion("Reconstruccion del libro: el hueco de secuencia es irrecuperable")
    libro = OrderBook("BTCUSD")
    libro.apply_snapshot(
        sequence=1000,
        bids=[Level(99.5, 300.0), Level(99.0, 500.0), Level(98.5, 700.0)],
        asks=[Level(100.5, 200.0), Level(101.0, 400.0), Level(101.5, 600.0)],
    )
    print(f"snapshot en secuencia {libro.last_sequence}: estado {libro.state.value}")
    print(f"  mejor compra {libro.best_bid}  mejor venta {libro.best_ask}")
    print(f"  diferencial {libro.spread:.2f}  medio {libro.mid:.3f}  "
          f"microprecio {libro.microprice:.3f}")
    print(f"  desequilibrio a 3 niveles: {libro.imbalance(3):+.4f}")
    print("\n  El microprecio esta POR ENCIMA del medio porque la venta tiene menos")
    print("  volumen (200 contra 300): es el lado que se agota primero.\n")

    libro.apply_delta(1001, Side.BID, 99.75, 150.0)
    libro.apply_delta(1002, Side.ASK, 100.25, 80.0)
    print(f"tras 2 deltas en orden: diferencial {libro.spread:.2f}, "
          f"estado {libro.state.value}")

    print("\nAhora se pierden los mensajes 1003, 1004 y 1005:")
    try:
        libro.apply_delta(1006, Side.BID, 99.80, 100.0)
    except GapDetected as e:
        print(f"  GapDetected: {e}")
    print(f"  estado: {libro.state.value}   valido: {libro.is_valid}")

    print("\nY los deltas siguientes tambien se rechazan, aunque su secuencia sea")
    print("contigua: el estado es irrecuperable sin snapshot.")
    try:
        libro.apply_delta(1007, Side.BID, 99.80, 100.0)
    except GapDetected as e:
        print(f"  GapDetected: {str(e)[:70]}...")

    print("\nRecuperacion por snapshot:")
    libro.apply_snapshot(
        sequence=1100, bids=[Level(99.9, 100.0)], asks=[Level(100.1, 120.0)]
    )
    print(f"  estado {libro.state.value}, secuencia {libro.last_sequence}, "
          f"diferencial {libro.spread:.2f}")
    print(f"  acepta deltas de nuevo: {libro.apply_delta(1101, Side.BID, 99.95, 50.0)}")

    print("\nDeltas en vuelo anteriores al snapshot: se descartan, no es un error.")
    print(f"  delta 1050 (anterior a 1100): aplicado = "
          f"{libro.apply_delta(1050, Side.BID, 1.0, 1.0)}")

    print("\nLibro cruzado: una compra por encima de la mejor venta.")
    libro.apply_delta(1102, Side.BID, 100.50, 10.0)
    print(f"  cruzado: {libro.is_crossed}   diferencial: {libro.spread:+.2f}")
    print("  Un diferencial negativo no es un precio: es un estado incoherente.")

    print(f"\nEstadisticas de integridad del feed:")
    for k, v in libro.stats().items():
        print(f"  {k:>18}: {v}")


def main() -> None:
    demo_latencia_vs_throughput()
    demo_promedios()
    demo_false_sharing()
    demo_libro()


if __name__ == "__main__":
    main()
