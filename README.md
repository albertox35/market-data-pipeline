# market-data-pipeline

Pipeline de market data de baja latencia: **ring buffer SPSC wait-free** en C++20
con relleno a línea de caché y ordenamiento de memoria acquire/release explícito,
reconstrucción del libro L2 con **detección de huecos de secuencia**, y medición
de latencia en **percentiles**.

```python
from mdpipeline import measure_latency, measure_throughput, clock_resolution_ns

print(clock_resolution_ns())                    # 100 ns en Windows
print(measure_latency(n_messages=1_000_000, target_rate_per_sec=500_000))
# p50 107 ns | p90 205 | p99 205 | ... [p50 <= 1 tic de reloj de 100 ns]
print(measure_throughput(n_messages=6_000_000)) # 16.3M msg/s
```

---

## 1. Latencia y throughput no se miden en la misma corrida

Es el error de medición más común en este tipo de componentes, y el repositorio
lo separa en dos funciones distintas precisamente para que no se pueda cometer.

| Configuración | Throughput | Saturada | «Latencia» p50 | Prevista `capacidad/throughput` |
|---|---|---|---|---|
| Cola saturada, capacidad 16 384 | 16.3M msg/s | **sí** | **866 µs** | 1 007 µs |
| Cola superficial, 500 k msg/s pautados | — | no (profundidad máx. 506) | **107 ns** | — |

Si el productor corre a máxima velocidad, la cola se queda permanentemente llena
y cada mensaje espera a que se consuman los que tiene delante. La «latencia»
medida vale entonces `capacidad / throughput` —866 µs frente a los 1 007 µs
previstos, el mismo orden— y **no dice nada sobre el costo del traspaso**, que es
unas **8 000 veces menor**.

`measure_latency` paca al productor con espera activa y devuelve
`max_queue_depth` y `saturated` para que se pueda comprobar que la medición es
válida. `measure_throughput` satura a propósito.

## 2. El promedio de latencia no sirve

La distribución tiene cola muy pesada. Dos sistemas con el mismo promedio pueden
tener colas que difieren en órdenes de magnitud, y es la cola la que determina si
el sistema sirve. Razón p99.9 / p50 medida: **más de 100×**.

**Advertencia honesta:** en una máquina sin núcleos aislados ni kernel ajustado,
la cola por encima de p99.9 está dominada por el planificador del sistema
operativo, no por el ring buffer. Los p99.99 de milisegundos que aparecen en los
benchmarks son cambios de contexto. Para medir el componente hay que fijar
afinidad de núcleo y aislarlos del planificador; eso no está hecho aquí y los
números lo reflejan.

## 3. La medición está limitada por la resolución del reloj

Resolución efectiva de `steady_clock` en esta máquina: **100 ns exactos**.

El p50 medido de **107 ns es un tic de reloj**. Eso significa que el costo real
del traspaso está en algún punto **por debajo de 100 ns** y este instrumento no
puede resolverlo. `LatencyProfile.resolution_limited` lo señala y `__str__` lo
imprime.

Reportar «p50 = 107 ns» sin decir que la resolución es de 100 ns sería presentar
un artefacto del instrumento como una medición. Para resolver por debajo haría
falta el contador de ciclos (`rdtsc`) con su factor de conversión calibrado.

## 4. Relleno a línea de caché: el costo del *false sharing*, medido

| Capacidad | Con relleno | Sin relleno | Ganancia |
|---|---|---|---|
| 1 024 | 14.2M msg/s | 11.3M msg/s | **+24.9 %** |
| 8 192 | 13.0M msg/s | 11.6M msg/s | +12.6 % |

Si los índices de lectura y escritura caen en la misma línea de 64 bytes, cada
escritura de un hilo invalida la línea en el núcleo del otro. Los dos hilos **no
comparten datos**, pero el protocolo de coherencia actúa como si lo hicieran. El
parámetro `padded` existe para poder medir ese costo, no porque tenga sentido
desactivarlo.

## 5. Las cuatro decisiones concretas del ring buffer

«Colas libres de bloqueos» es el término correcto pero no dice nada. Lo concreto:

1. **SPSC, un productor y un consumidor.** Es el único caso realmente
   *wait-free*: cada hilo escribe un índice y lee el del otro, sin bucle de
   reintento ni read-modify-write. Una cola MPMC necesita compare-and-swap en
   bucle y entonces ya no es wait-free: un hilo puede reintentar
   indefinidamente. En un feed de market data el caso SPSC es el real: un hilo
   de red produce, un hilo de proceso consume.
2. **Relleno a línea de caché**, medido arriba.
3. **Acquire/release, no `seq_cst`.** El valor por omisión de `std::atomic` es
   `memory_order_seq_cst`, que en x86 obliga a un `MFENCE` por escritura.
   Acquire/release da exactamente la garantía necesaria —que lo escrito en el
   buffer sea visible antes de publicar el índice— sin la barrera total.
4. **Ninguna asignación de memoria en el camino crítico.** El buffer se asigna
   una vez en el constructor. Una sola llamada a `malloc` que toque el sistema
   operativo cuesta microsegundos y arruina el presupuesto completo.

Capacidad obligatoriamente potencia de dos: permite sustituir el módulo por una
máscara de bits, que cuesta un ciclo.

## 6. El hueco de secuencia es irrecuperable

Un feed incremental envía deltas. Si se pierde un mensaje, todos los posteriores
se aplican sobre un estado incorrecto: **el libro no se degrada, queda mal y
sigue pareciendo válido**. Es el error más grave posible en este componente,
porque produce precios erróneos sin ninguna señal.

```
GapDetected: hueco de secuencia: se esperaba 1003 y llego 1006;
faltan 3 mensajes. El libro queda invalidado hasta el proximo snapshot.
```

Y los deltas siguientes **también** se rechazan, aunque su secuencia sea
contigua: el estado es irrecuperable sin snapshot. La única vía de recuperación
es reconstruir desde cero.

Los deltas anteriores al snapshot se descartan **sin error**: es el caso normal
de los mensajes en vuelo mientras se pedía el snapshot.

### Libro cruzado

Si la mejor compra alcanza o supera la mejor venta, el estado es incoherente.
Publicar un diferencial negativo como si fuera un precio es un error en
cualquier caso, así que `is_crossed` lo detecta.

### Microprecio

```
P_b·S_a + P_a·S_b
─────────────────
    S_a + S_b
```

Pondera **al contrario** de lo que sugiere la intuición: el precio se acerca al
lado con **menos** volumen, porque ese es el que se agota primero. Con compra
99.5 × 300 y venta 100.5 × 200, el medio es 100.000 y el microprecio **100.100**:
por encima, hacia la venta, que tiene menos volumen.

---

## Validación

36 pruebas, separadas en dos grupos. Reproducible con
`python examples/pipeline_demo.py`.

**29 pruebas deterministas** (corrección: FIFO, pérdidas, libro, huecos,
recuperación) corren en CI y tardan 0.12 s.

**7 pruebas marcadas `@pytest.mark.timing`** miden rendimiento, así que dependen
de la carga de la máquina. **CI las excluye** (`pytest -m "not timing"`) porque en
un runner compartido fallarían de forma intermitente, y una suite que falla a
veces es peor que no tenerla: deja de ser evidencia. Se ejecutan a mano en una
máquina controlada con `pytest -m timing`.

Esa separación es también la razón por la que las pruebas de rendimiento que
quedan solo afirman sobre la **mediana** y sobre la resolución del reloj, que son
robustas, y no sobre los percentiles altos, que están dominados por el
planificador del sistema operativo.

| Propiedad verificada | Resultado |
|---|---|
| Capacidad potencia de dos obligatoria | lanza excepción |
| Orden FIFO preservado | exacto |
| Sin pérdidas ni duplicados alternando encolado y desencolado | exacto |
| Cola llena rechaza, cola vacía devuelve `None` | sí |
| El tick (40 B) cabe en una línea de caché (64 B) | sí |
| Percentiles monótonos `p50 ≤ p90 ≤ p99 ≤ p99.9 ≤ p99.99` | sí |
| Con tasa pautada la cola no se satura | profundidad 506 de 8192 |
| Latencia de traspaso submicrosegundo | p50 ≤ 1 tic de reloj |
| Cero mensajes fuera de orden en 300 000 | sí |
| La espera saturada es ~`capacidad/throughput` y >100× el traspaso | sí |
| `measure_latency` rechaza tasa cero | lanza excepción |
| Un hueco invalida el libro y bloquea deltas posteriores | sí |
| El snapshot recupera y vuelve a aceptar deltas | sí |
| Deltas anteriores al snapshot se descartan sin error | sí |
| Libro cruzado detectado | sí |
| Microprecio hacia el lado de menos volumen | exacto |
| Volumen cero elimina el nivel | sí |

---

## Qué no hace

- **No hay decodificador de protocolo real.** No hay ITCH, SBE ni FAST. El tick
  es una estructura propia de tamaño fijo. Un decodificador de verdad exige la
  especificación del mercado concreto y datos de referencia para probarlo.
- **No hay ingesta de red.** No hay multicast UDP, ni recuperación por el canal
  de retransmisión, ni manejo de los feeds A/B que publican los mercados en
  paralelo para tolerar pérdidas.
- **No hay *timestamping* de hardware.** La distinción entre la marca de tiempo
  de la NIC y la de software es de microsegundos y es la métrica que de verdad
  importa en HFT; requiere una tarjeta con PTP y acceso a sus registros.
- **No hay afinidad de núcleo ni aislamiento del planificador**, que es lo que
  domina la cola por encima de p99.9. Los números del README lo reflejan en
  lugar de ocultarlo.
- **No hay persistencia asíncrona implementada.** El diseño está descrito —hilo
  escritor aparte que consume del ring buffer, con el camino crítico escribiendo
  solo a memoria— pero no está escrito. Escribir a disco es una llamada
  bloqueante, así que «lock-free» y «almacenamiento en tiempo real» están en
  tensión y resolverlo bien es un componente propio.
- **Solo L2 agregado por nivel de precio.** No hay L3 con órdenes individuales ni
  prioridad de cola.
- **`rdtsc` no está implementado.** Es lo que haría falta para resolver por
  debajo de los 100 ns del reloj del sistema, con su calibración y el manejo de
  TSC no invariante entre núcleos.
- **Un solo instrumento por libro.** No hay gestión de miles de símbolos en
  paralelo ni asignación de hilos por partición.

---

## Alcance y decisiones de diseño

**El ring buffer no se opera desde Python para medir rendimiento.** Cruzar la
frontera de pybind11 y adquirir el GIL cuesta cientos de nanosegundos, tres
órdenes de magnitud por encima de la operación que se quiere medir. La clase
`RingBuffer` expuesta a Python existe para verificar **corrección** —orden FIFO,
ausencia de pérdidas, comportamiento en los límites— no rendimiento. El
benchmark corre productor y consumidor enteros en C++ con el GIL liberado.

**La espera de pacing es activa, no `sleep`.** Dormir tiene granularidad de
milisegundos, tres órdenes de magnitud por encima del intervalo que hay que
respetar para pautar a cientos de miles de mensajes por segundo.

**Un mensaje nunca se descarta por cola llena: se reintenta.** En market data
descartar un mensaje rompe la reconstrucción del libro, que es precisamente el
problema que el repositorio se toma en serio.

**El repositorio es autocontenido.** No depende de una librería núcleo compartida
con los demás del conjunto.

---

## Instalación

Requiere un compilador con C++20.

```bash
pip install -e ".[dev]"
```

```bash
python -m pytest -q
```

```bash
python examples/pipeline_demo.py
```

Probado con MSVC 14.51 (Visual Studio 2026 Build Tools). Si setuptools no detecta
una instalación reciente de Visual Studio, preconfigurar el entorno con
`vcvars64.bat` y `DISTUTILS_USE_SDK=1`.

---

## Referencias

- Williams, A. (2019). *C++ Concurrency in Action*, 2ª ed. Manning. Capítulo 5
  para el modelo de memoria y capítulo 7 para estructuras lock-free.
- Fog, A. *Optimizing software in C++* y *The microarchitecture of Intel, AMD and
  VIA CPUs*. Para el costo de las barreras y del *false sharing*.
- Intel. *Intel 64 and IA-32 Architectures Optimization Reference Manual*,
  sección de coherencia de caché.
- Tene, G. *How NOT to measure latency*. Charla sobre el *coordinated omission*
  y por qué los promedios de latencia engañan.
- Nasdaq. *TotalView-ITCH 5.0 Specification*. Protocolo de referencia de un feed
  incremental con secuenciación.
- FIX Trading Community. *Simple Binary Encoding (SBE) v1.0*.
- Gatheral, J. & Oomen, R. (2010). *Zero-intelligence realized variance
  estimation*. Para el uso del microprecio.
- Stoikov, S. (2018). *The micro-price: a high-frequency estimator of future
  prices*. Quantitative Finance 18(12).

## Licencia

MIT. Ver [LICENSE](LICENSE).
