"""Validacion del ring buffer, la medicion de latencia y la reconstruccion del libro.

Las pruebas marcadas con ``@pytest.mark.timing`` miden **rendimiento**, asi que
dependen de la carga de la maquina. En un runner de CI compartido fallarian de
forma intermitente, y una suite que falla a veces es peor que no tenerla: deja de
ser evidencia. Por eso CI ejecuta ``pytest -m "not timing"``, que es
deterministico, y las de rendimiento se corren a mano en una maquina controlada:

    python -m pytest -m timing
"""

from __future__ import annotations

import pytest

from mdpipeline import (
    CACHE_LINE_SIZE,
    TICK_SIZE_BYTES,
    BookState,
    GapDetected,
    Level,
    OrderBook,
    RingBuffer,
    Side,
    clock_resolution_ns,
    measure_latency,
    measure_throughput,
)


# ------------------------------------------------------------- ring buffer


def test_la_capacidad_debe_ser_potencia_de_dos():
    """Permite sustituir el modulo por una mascara de bits, que cuesta un ciclo."""
    with pytest.raises(ValueError, match="potencia de dos"):
        RingBuffer(1000)
    with pytest.raises(ValueError, match="potencia de dos"):
        RingBuffer(1)
    assert RingBuffer(1024).capacity == 1024


def test_el_orden_fifo_se_preserva():
    cola = RingBuffer(64)
    for i in range(50):
        assert cola.push(i, 100.0 + i, 10.0, 1)
    for i in range(50):
        t = cola.pop()
        assert t is not None
        assert t["sequence"] == i
        assert t["price"] == pytest.approx(100.0 + i)


def test_no_se_pierde_ningun_mensaje_al_alternar():
    """Intercalar encolado y desencolado no debe perder ni duplicar."""
    cola = RingBuffer(8)
    recibidos = []
    siguiente = 0
    for ronda in range(100):
        for _ in range(3):
            if cola.push(siguiente, 100.0, 1.0, 1):
                siguiente += 1
        while (t := cola.pop()) is not None:
            recibidos.append(t["sequence"])
    while (t := cola.pop()) is not None:
        recibidos.append(t["sequence"])
    assert recibidos == list(range(siguiente))


def test_la_cola_vacia_devuelve_none():
    assert RingBuffer(16).pop() is None


def test_la_cola_llena_rechaza_el_encolado():
    cola = RingBuffer(4)
    for i in range(4):
        assert cola.push(i, 100.0, 1.0, 1)
    assert not cola.push(99, 100.0, 1.0, 1)
    assert cola.size == 4
    # Al liberar un hueco vuelve a aceptar.
    assert cola.pop() is not None
    assert cola.push(99, 100.0, 1.0, 1)


def test_el_tamanio_refleja_el_contenido():
    cola = RingBuffer(32)
    assert cola.size == 0
    for i in range(10):
        cola.push(i, 100.0, 1.0, 1)
    assert cola.size == 10
    for _ in range(4):
        cola.pop()
    assert cola.size == 6


def test_el_tick_cabe_holgadamente_en_una_linea_de_cache():
    """40 bytes en una linea de 64: un tick nunca se parte entre dos lineas."""
    assert CACHE_LINE_SIZE == 64
    assert TICK_SIZE_BYTES <= CACHE_LINE_SIZE


# --------------------------------------------------------- medicion de latencia


def test_medir_latencia_exige_una_tasa_objetivo():
    """Con tasa cero el productor satura y lo medido es espera, no latencia."""
    with pytest.raises(ValueError, match="tasa objetivo positiva"):
        measure_latency(n_messages=1000, target_rate_per_sec=0.0)


@pytest.mark.timing
def test_los_percentiles_de_latencia_son_monotonos():
    p = measure_latency(n_messages=200_000, target_rate_per_sec=400_000.0)
    assert p.p50_ns <= p.p90_ns <= p.p99_ns <= p.p999_ns <= p.p9999_ns
    assert p.min_ns <= p.p50_ns
    assert p.p9999_ns <= p.max_ns


@pytest.mark.timing
def test_con_tasa_pautada_la_cola_no_se_satura():
    """Condicion para que la medicion sea de latencia y no de espera en cola."""
    p = measure_latency(n_messages=200_000, target_rate_per_sec=300_000.0, capacity=8192)
    assert not p.saturated
    assert p.is_measurement_valid
    assert p.max_queue_depth < p.capacity


@pytest.mark.timing
def test_la_latencia_de_traspaso_es_de_orden_submicrosegundo():
    """El ring buffer traspasa en un tic de reloj, no en microsegundos.

    Solo se afirma sobre la **mediana**. Los percentiles altos no se acotan a
    proposito: en una maquina sin nucleos aislados estan dominados por el
    planificador del sistema operativo, no por el ring buffer, y un umbral sobre
    ellos seria una prueba de la carga de la maquina disfrazada de prueba del
    componente. Es la misma salvedad que declara el README.
    """
    p = measure_latency(n_messages=300_000, target_rate_per_sec=400_000.0)
    resolucion = clock_resolution_ns()
    assert 0 <= p.p50_ns <= 3 * resolucion, f"p50 = {p.p50_ns} ns (resolucion {resolucion})"
    assert p.p50_ns <= p.p90_ns


@pytest.mark.timing
def test_no_hay_mensajes_fuera_de_orden():
    p = measure_latency(n_messages=300_000, target_rate_per_sec=500_000.0)
    assert p.out_of_order == 0
    assert p.count == 300_000


@pytest.mark.timing
def test_el_promedio_difiere_de_la_mediana_y_por_eso_no_sirve():
    """La razon de reportar percentiles.

    Con una distribucion de cola pesada el promedio se separa de la mediana. Si
    coincidieran, reportar el promedio seria inocuo; no coinciden.
    """
    p = measure_latency(n_messages=300_000, target_rate_per_sec=400_000.0)
    assert p.tail_ratio > 2.0, f"razon p99.9/p50 = {p.tail_ratio}"


@pytest.mark.timing
def test_la_latencia_saturada_es_espera_en_cola_y_no_traspaso():
    """La distincion central de la medicion.

    Con la cola saturada la latencia observada es del orden de
    ``capacidad / throughput``, cuatro ordenes de magnitud por encima del costo
    real del traspaso. Medir latencia saturando la cola es medir otra cosa.
    """
    t = measure_throughput(n_messages=6_000_000, capacity=16_384)
    assert t.saturated
    assert t.max_queue_depth == t.capacity

    # La espera observada es del MISMO ORDEN que la prevista por la capacidad.
    # No se exige una coincidencia estrecha a proposito: la prediccion supone la
    # cola llena de forma continua, y en la practica el consumidor sigue el ritmo
    # durante parte de la corrida, asi que lo observado queda por debajo. Cuanta
    # parte depende de la carga de la maquina, que no se controla aqui.
    razon = t.queueing_latency_p50_ns / t.expected_queueing_latency_ns
    assert 0.3 < razon < 1.5, f"razon observada/prevista = {razon}"

    # Y es ordenes de magnitud mayor que el traspaso. La comparacion se ancla a
    # la **resolucion del reloj**, no al p50 del traspaso: ese p50 se degrada
    # bajo carga de la maquina --es la salvedad del planificador documentada en
    # el README-- y usarlo como referencia haria la prueba fragil. La resolucion
    # del reloj es deterministica y basta para el argumento: el traspaso cabe en
    # un tic, la espera en cola necesita cientos.
    resolucion = clock_resolution_ns()
    assert t.queueing_latency_p50_ns > 100 * resolucion

    traspaso = measure_latency(n_messages=200_000, target_rate_per_sec=400_000.0)
    assert traspaso.resolution_limited, "el traspaso debe caber en un tic de reloj"


@pytest.mark.timing
def test_el_throughput_supera_el_millon_de_mensajes_por_segundo():
    t = measure_throughput(n_messages=2_000_000, capacity=4096)
    assert t.messages_per_second > 1e6
    assert t.elapsed_s > 0.0


# ----------------------------------------------------------- libro de ordenes


def libro_inicializado(strict: bool = True) -> OrderBook:
    libro = OrderBook("TEST", strict=strict)
    libro.apply_snapshot(
        sequence=100,
        bids=[Level(99.5, 300.0), Level(99.0, 500.0), Level(98.5, 700.0)],
        asks=[Level(100.5, 200.0), Level(101.0, 400.0), Level(101.5, 600.0)],
    )
    return libro


def test_un_libro_nuevo_esta_sin_inicializar():
    libro = OrderBook("TEST")
    assert libro.state is BookState.UNINITIALISED
    assert not libro.is_valid
    assert libro.best_bid is None and libro.best_ask is None


def test_no_se_pueden_aplicar_deltas_sin_snapshot():
    libro = OrderBook("TEST")
    with pytest.raises(GapDetected, match="sin inicializar"):
        libro.apply_delta(1, Side.BID, 99.0, 100.0)


def test_el_snapshot_establece_el_libro():
    libro = libro_inicializado()
    assert libro.is_valid
    assert libro.last_sequence == 100
    assert libro.best_bid == Level(99.5, 300.0)
    assert libro.best_ask == Level(100.5, 200.0)
    assert libro.spread == pytest.approx(1.0)
    assert libro.mid == pytest.approx(100.0)
    assert libro.n_levels == (3, 3)


def test_los_deltas_en_orden_actualizan_el_libro():
    libro = libro_inicializado()
    assert libro.apply_delta(101, Side.BID, 99.75, 150.0)
    assert libro.best_bid == Level(99.75, 150.0)
    assert libro.apply_delta(102, Side.ASK, 100.25, 80.0)
    assert libro.best_ask == Level(100.25, 80.0)
    assert libro.spread == pytest.approx(0.5)
    assert libro.applied_deltas == 2


def test_un_volumen_cero_elimina_el_nivel():
    libro = libro_inicializado()
    assert libro.apply_delta(101, Side.BID, 99.5, 0.0)
    assert libro.best_bid == Level(99.0, 500.0)
    assert libro.n_levels == (2, 3)


def test_un_hueco_de_secuencia_invalida_el_libro():
    """La propiedad mas importante del modulo.

    Seguir aplicando deltas tras un hueco produce precios erroneos **sin
    ninguna senial**: el libro no se degrada, queda mal y sigue pareciendo
    valido.
    """
    libro = libro_inicializado()
    assert libro.apply_delta(101, Side.BID, 99.75, 150.0)
    with pytest.raises(GapDetected, match="hueco de secuencia"):
        libro.apply_delta(105, Side.BID, 99.80, 100.0)  # faltan 102, 103, 104
    assert libro.state is BookState.STALE
    assert not libro.is_valid
    assert libro.gap_count == 1


def test_el_mensaje_del_hueco_dice_cuantos_faltan():
    libro = libro_inicializado()
    with pytest.raises(GapDetected, match="faltan 4 mensajes"):
        libro.apply_delta(105, Side.BID, 99.8, 100.0)


def test_un_libro_invalidado_rechaza_deltas_posteriores():
    libro = libro_inicializado()
    with pytest.raises(GapDetected):
        libro.apply_delta(105, Side.BID, 99.8, 100.0)
    # El siguiente delta, aunque su secuencia sea contigua, tambien se rechaza:
    # el estado sigue siendo irrecuperable sin snapshot.
    with pytest.raises(GapDetected, match="invalidado por un hueco previo"):
        libro.apply_delta(106, Side.BID, 99.8, 100.0)


def test_el_snapshot_recupera_un_libro_invalidado():
    """La unica via de recuperacion: reconstruir desde cero."""
    libro = libro_inicializado()
    with pytest.raises(GapDetected):
        libro.apply_delta(105, Side.BID, 99.8, 100.0)
    assert libro.state is BookState.STALE

    libro.apply_snapshot(
        sequence=200,
        bids=[Level(99.9, 100.0)],
        asks=[Level(100.1, 100.0)],
    )
    assert libro.is_valid
    assert libro.last_sequence == 200
    assert libro.best_bid == Level(99.9, 100.0)
    assert libro.snapshot_count == 2
    # Y vuelve a aceptar deltas.
    assert libro.apply_delta(201, Side.BID, 99.95, 50.0)


def test_los_deltas_anteriores_al_snapshot_se_descartan_sin_error():
    """Caso normal: mensajes en vuelo mientras se pedia el snapshot."""
    libro = libro_inicializado()
    assert libro.apply_delta(50, Side.BID, 99.0, 999.0) is False
    assert libro.apply_delta(100, Side.BID, 99.0, 999.0) is False
    assert libro.discarded_deltas == 2
    assert libro.is_valid  # no es un error
    assert libro.best_bid == Level(99.5, 300.0)  # el libro no cambio


def test_en_modo_no_estricto_el_hueco_se_registra_sin_excepcion():
    libro = libro_inicializado(strict=False)
    assert libro.apply_delta(105, Side.BID, 99.8, 100.0) is False
    assert libro.state is BookState.STALE
    assert libro.gap_count == 1


def test_se_detecta_un_libro_cruzado():
    """Un diferencial negativo no es un precio: es un estado incoherente."""
    libro = libro_inicializado()
    assert not libro.is_crossed
    # Una compra por encima de la mejor venta.
    assert libro.apply_delta(101, Side.BID, 101.25, 100.0)
    assert libro.is_crossed
    assert libro.spread < 0.0


def test_la_profundidad_respeta_la_prioridad_de_precio():
    libro = libro_inicializado()
    compras = libro.depth(Side.BID, 3)
    ventas = libro.depth(Side.ASK, 3)
    assert [lv.price for lv in compras] == [99.5, 99.0, 98.5]
    assert [lv.price for lv in ventas] == [100.5, 101.0, 101.5]


def test_la_profundidad_se_limita_a_los_niveles_pedidos():
    libro = libro_inicializado()
    assert len(libro.depth(Side.BID, 2)) == 2
    assert len(libro.depth(Side.BID, 10)) == 3  # solo hay 3
    with pytest.raises(ValueError, match="al menos 1"):
        libro.depth(Side.BID, 0)


def test_el_microprecio_se_inclina_hacia_el_lado_con_menos_volumen():
    """Pondera al contrario de lo que sugiere la intuicion.

    El lado con menos volumen se agota primero, asi que el precio tiende hacia
    el. Aqui la venta tiene 200 y la compra 300: el microprecio queda **por
    encima** del medio.
    """
    libro = libro_inicializado()
    assert libro.mid == pytest.approx(100.0)
    micro = libro.microprice
    assert micro is not None
    assert micro > libro.mid
    esperado = (99.5 * 200.0 + 100.5 * 300.0) / 500.0
    assert micro == pytest.approx(esperado)


def test_el_desequilibrio_tiene_el_signo_correcto():
    libro = libro_inicializado()
    # Compras 300+500+700 = 1500; ventas 200+400+600 = 1200.
    des = libro.imbalance(3)
    assert des is not None
    assert des == pytest.approx((1500.0 - 1200.0) / 2700.0)
    assert des > 0.0  # mas presion de compra


def test_el_desequilibrio_de_un_libro_vacio_no_esta_definido():
    libro = OrderBook("TEST")
    libro.apply_snapshot(0, [], [])
    assert libro.imbalance() is None
    assert libro.microprice is None
    assert libro.spread is None


def test_las_estadisticas_registran_la_integridad_del_feed():
    libro = libro_inicializado(strict=False)
    libro.apply_delta(101, Side.BID, 99.6, 100.0)
    libro.apply_delta(105, Side.BID, 99.7, 100.0)  # hueco
    s = libro.stats()
    assert s["applied_deltas"] == 1
    assert s["gap_count"] == 1
    assert s["snapshot_count"] == 1
    assert s["state"] == BookState.STALE.value
    assert 0.0 < float(s["gap_rate"]) <= 1.0


def test_el_libro_rechaza_volumenes_negativos():
    libro = libro_inicializado()
    with pytest.raises(ValueError, match="no puede ser negativo"):
        libro.apply_delta(101, Side.BID, 99.0, -5.0)


def test_el_snapshot_rechaza_secuencias_negativas():
    libro = OrderBook("TEST")
    with pytest.raises(ValueError, match="no negativo"):
        libro.apply_snapshot(-1, [], [])


def test_el_snapshot_ignora_niveles_de_volumen_nulo():
    libro = OrderBook("TEST")
    libro.apply_snapshot(0, [Level(99.0, 100.0), Level(98.0, 0.0)], [Level(101.0, 100.0)])
    assert libro.n_levels == (1, 1)
