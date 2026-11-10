// Ring buffer SPSC wait-free y harness de medicion de latencia.
//
// «Colas libres de bloqueos» es el termino correcto pero no dice nada. Lo
// concreto son cuatro decisiones, y cada una tiene una razon medible:
//
// 1. **SPSC, un productor y un consumidor.** Es el unico caso realmente
//    *wait-free*: cada hilo escribe un solo indice y lee el del otro, asi que no
//    hay bucle de reintento ni operacion read-modify-write. Una cola MPMC
//    necesita compare-and-swap en bucle, y entonces ya no es wait-free: un hilo
//    puede reintentar indefinidamente. En un feed de market data el caso SPSC es
//    el que se da de verdad: un hilo de red produce, un hilo de proceso consume.
//
// 2. **Relleno a linea de cache.** Si los indices de lectura y escritura caen en
//    la misma linea de 64 bytes, cada escritura de uno invalida la linea en el
//    nucleo del otro. Es *false sharing*: los dos hilos no comparten datos pero
//    el protocolo de coherencia actua como si lo hicieran, y el costo es del
//    orden de 100 ciclos por operacion. El harness permite medir la diferencia.
//
// 3. **Ordenamiento de memoria acquire/release, no seq_cst.** El valor por
//    omision de std::atomic es memory_order_seq_cst, que en x86 obliga a una
//    instruccion MFENCE en cada escritura. Acquire/release da exactamente la
//    garantia que hace falta --que lo escrito en el buffer sea visible antes de
//    que se publique el indice-- sin la barrera total.
//
// 4. **Ninguna asignacion de memoria en el camino critico.** El buffer se asigna
//    una vez en el constructor. Una sola llamada a malloc en el camino critico
//    puede costar microsegundos si toca el sistema operativo, lo que arruina el
//    presupuesto de latencia completo.
//
// Y la regla de medicion que gobierna todo esto: **la latencia se reporta en
// percentiles, nunca en promedio**. La distribucion tiene cola muy pesada; el
// promedio esconde justo lo que importa.

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <limits>
#include <new>
#include <stdexcept>
#include <thread>
#include <vector>

namespace py = pybind11;

namespace {

// Tamanio de linea de cache. std::hardware_destructive_interference_size seria
// lo correcto, pero no esta disponible en todas las bibliotecas estandar, asi
// que se usa 64 con respaldo.
#ifdef __cpp_lib_hardware_interference_size
constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;
#else
constexpr std::size_t kCacheLine = 64;
#endif

// Un tick de market data. Tamanio fijo y trivialmente copiable: el buffer puede
// moverlo con una copia de bytes, sin constructores ni asignaciones.
struct Tick {
    std::uint64_t sequence;
    std::int64_t timestamp_ns;  // reloj del productor, para medir latencia
    double price;
    double size;
    std::int32_t side;  // +1 compra, -1 venta
    std::int32_t padding;
};
static_assert(std::is_trivially_copyable_v<Tick>, "el tick debe ser copiable por bytes");

// Ring buffer SPSC.
//
// `padded` controla si los indices se separan en lineas de cache distintas.
// Existe como parametro **para poder medir el costo del false sharing**, no
// porque tenga sentido desactivarlo en produccion.
template <bool Padded>
class SPSCQueue {
   public:
    explicit SPSCQueue(std::size_t capacity) : capacity_(capacity), mask_(capacity - 1) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument(
                "la capacidad debe ser una potencia de dos mayor o igual a 2: permite "
                "sustituir el modulo por una mascara de bits, que cuesta un ciclo");
        }
        buffer_ = std::make_unique<Tick[]>(capacity);
        write_.store(0, std::memory_order_relaxed);
        read_.store(0, std::memory_order_relaxed);
    }

    // Encola sin bloquear. Devuelve false si la cola esta llena.
    //
    // El orden importa: primero se escribe el dato, despues se publica el indice
    // con release. Eso garantiza que ningun consumidor pueda ver el indice nuevo
    // sin ver tambien el dato, que es exactamente la garantia que hace falta.
    bool try_push(const Tick& value) noexcept {
        const std::size_t w = write_.load(std::memory_order_relaxed);
        const std::size_t next = w + 1;
        // acquire sobre el indice del otro hilo: lo que el consumidor libero ya
        // es visible.
        if (next - read_.load(std::memory_order_acquire) > capacity_) {
            return false;  // llena
        }
        buffer_[w & mask_] = value;
        write_.store(next, std::memory_order_release);
        return true;
    }

    bool try_pop(Tick& out) noexcept {
        const std::size_t r = read_.load(std::memory_order_relaxed);
        if (r == write_.load(std::memory_order_acquire)) {
            return false;  // vacia
        }
        out = buffer_[r & mask_];
        read_.store(r + 1, std::memory_order_release);
        return true;
    }

    std::size_t size() const noexcept {
        return write_.load(std::memory_order_acquire) - read_.load(std::memory_order_acquire);
    }

    std::size_t capacity() const noexcept { return capacity_; }

   private:
    std::size_t capacity_;
    std::size_t mask_;
    std::unique_ptr<Tick[]> buffer_;

    // Los indices van en lineas de cache separadas si Padded. Sin relleno los
    // dos atomicos comparten linea y cada escritura de uno invalida la del otro.
    alignas(Padded ? kCacheLine : alignof(std::atomic<std::size_t>))
        std::atomic<std::size_t> write_;
    char pad_[Padded ? kCacheLine : 1]{};
    alignas(Padded ? kCacheLine : alignof(std::atomic<std::size_t>))
        std::atomic<std::size_t> read_;
};

// Histograma de latencias con cubos logaritmicos.
//
// Un histograma lineal no sirve: las latencias de interes van de decenas de
// nanosegundos a milisegundos, cinco ordenes de magnitud. Con cubos
// logaritmicos el error relativo es constante, que es la propiedad que se quiere
// cuando se reportan percentiles.
class LatencyHistogram {
   public:
    static constexpr int kBucketsPerDecade = 32;
    static constexpr int kDecades = 7;  // 1 ns a 10 ms
    static constexpr int kBuckets = kBucketsPerDecade * kDecades + 2;

    LatencyHistogram() : counts_(kBuckets, 0) {}

    void record(std::int64_t ns) noexcept {
        ++count_;
        if (ns < min_ || count_ == 1) min_ = ns;
        if (ns > max_) max_ = ns;
        sum_ += static_cast<double>(ns);
        counts_[bucket_of(ns)] += 1;
    }

    std::int64_t percentile(double p) const {
        if (count_ == 0) return 0;
        const std::uint64_t objetivo =
            static_cast<std::uint64_t>(p * static_cast<double>(count_) + 0.5);
        std::uint64_t acumulado = 0;
        for (int b = 0; b < kBuckets; ++b) {
            acumulado += counts_[b];
            if (acumulado >= objetivo) {
                // Se devuelve el borde superior del cubo, pero **acotado por el
                // maximo observado**. Sin ese tope, el borde del ultimo cubo
                // ocupado puede superar al maximo real --los cubos son
                // logaritmicos y por tanto anchos en la cola-- y entonces
                // p99.99 saldria mayor que el maximo, que es imposible por
                // definicion de percentil.
                return std::min(upper_bound_of(b), max_);
            }
        }
        return max_;
    }

    std::uint64_t count() const noexcept { return count_; }
    std::int64_t min() const noexcept { return count_ ? min_ : 0; }
    std::int64_t max() const noexcept { return max_; }
    double mean() const noexcept { return count_ ? sum_ / static_cast<double>(count_) : 0.0; }

   private:
    static int bucket_of(std::int64_t ns) noexcept {
        if (ns <= 0) return 0;
        const double l = std::log10(static_cast<double>(ns));
        const int b = static_cast<int>(l * kBucketsPerDecade) + 1;
        return std::clamp(b, 0, kBuckets - 1);
    }

    static std::int64_t upper_bound_of(int bucket) noexcept {
        if (bucket == 0) return 0;
        return static_cast<std::int64_t>(
            std::pow(10.0, static_cast<double>(bucket) / kBucketsPerDecade));
    }

    std::vector<std::uint64_t> counts_;
    std::uint64_t count_ = 0;
    std::int64_t min_ = 0;
    std::int64_t max_ = 0;
    double sum_ = 0.0;
};

inline std::int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Resolucion efectiva del reloj: minima diferencia no nula entre dos lecturas
// consecutivas.
//
// Es un dato imprescindible para interpretar cualquier medicion de latencia. En
// Windows `steady_clock` suele resolver alrededor de 100 ns; en Linux con
// CLOCK_MONOTONIC, decenas de nanosegundos. Si el traspaso del ring buffer
// cuesta menos que la resolucion, **no se puede medir con este reloj**: las
// lecturas de entrada y salida caen en el mismo tic y la diferencia sale cero.
//
// Reportar "p50 = 107 ns" sin decir que la resolucion es de 100 ns seria
// presentar un artefacto del instrumento como una medicion.
std::int64_t measure_clock_resolution_ns(int n_samples) {
    std::int64_t minimo = std::numeric_limits<std::int64_t>::max();
    for (int i = 0; i < n_samples; ++i) {
        const std::int64_t a = now_ns();
        std::int64_t b = now_ns();
        while (b == a) b = now_ns();
        minimo = std::min(minimo, b - a);
    }
    return minimo;
}

// Ejecuta productor y consumidor en hilos separados.
//
// `target_rate_per_sec` separa dos mediciones que NO se pueden obtener de la
// misma corrida, y confundirlas es el error de medicion mas comun en este tipo
// de componentes:
//
// - Con `target_rate_per_sec = 0` el productor corre a maxima velocidad. La cola
//   se **satura** y se queda permanentemente llena, asi que cada mensaje espera
//   a que se consuman los que tiene delante. La «latencia» medida vale entonces
//   `capacidad / throughput` y no dice nada sobre el costo del traspaso: con
//   capacidad 8192 y 13 millones de mensajes por segundo, salen 600
//   microsegundos, cinco ordenes de magnitud por encima de lo que cuesta la
//   operacion. Esta configuracion mide **throughput**.
//
// - Con una tasa objetivo por debajo de la capacidad de proceso, la cola se
//   mantiene superficial y la latencia medida si es el costo real del traspaso.
//   Esta configuracion mide **latencia**, y es la que corresponde al caso real:
//   el market data llega a la tasa que llega, no infinitamente rapido.
//
// Se devuelve la profundidad maxima observada de la cola precisamente para
// poder comprobar que no hubo saturacion.
template <bool Padded>
py::dict run_benchmark(std::size_t n_messages, std::size_t capacity, double target_rate_per_sec,
                       bool busy_wait) {
    SPSCQueue<Padded> cola(capacity);
    LatencyHistogram histograma;
    std::atomic<bool> listo{false};
    std::uint64_t fuera_de_orden = 0;
    std::size_t profundidad_maxima = 0;
    std::uint64_t reintentos_llena = 0;
    std::int64_t inicio_ns = 0;
    std::int64_t fin_ns = 0;

    {
        py::gil_scoped_release release;  // los hilos no tocan objetos de Python

        std::thread consumidor([&] {
            Tick t{};
            std::uint64_t recibidos = 0;
            std::uint64_t esperado = 0;
            listo.store(true, std::memory_order_release);
            while (recibidos < n_messages) {
                if (cola.try_pop(t)) {
                    histograma.record(now_ns() - t.timestamp_ns);
                    if (t.sequence != esperado) ++fuera_de_orden;
                    esperado = t.sequence + 1;
                    ++recibidos;
                } else if (!busy_wait) {
                    std::this_thread::yield();
                }
            }
        });

        while (!listo.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        const double intervalo_ns =
            target_rate_per_sec > 0.0 ? 1e9 / target_rate_per_sec : 0.0;
        inicio_ns = now_ns();
        Tick t{};
        for (std::uint64_t i = 0; i < n_messages; ++i) {
            if (intervalo_ns > 0.0) {
                // Espera activa hasta el instante de envio previsto. Se usa
                // espera activa y no sleep porque dormir tiene una
                // granularidad de milisegundos, tres ordenes de magnitud por
                // encima del intervalo que se quiere respetar.
                const std::int64_t objetivo =
                    inicio_ns + static_cast<std::int64_t>(static_cast<double>(i) * intervalo_ns);
                while (now_ns() < objetivo) {
                    // nada: mantener el nucleo caliente
                }
            }
            t.sequence = i;
            t.price = 100.0 + static_cast<double>(i % 100) * 0.01;
            t.size = 100.0;
            t.side = (i % 2) ? 1 : -1;
            // El sello de tiempo se toma lo mas tarde posible, justo antes de
            // publicar: cualquier trabajo previo no debe contar como latencia
            // de la cola.
            t.timestamp_ns = now_ns();
            while (!cola.try_push(t)) {
                // Cola llena: el consumidor no da abasto. Se reintenta en lugar
                // de descartar, porque en market data descartar un mensaje
                // rompe la reconstruccion del libro.
                ++reintentos_llena;
                if (!busy_wait) std::this_thread::yield();
            }
            const std::size_t profundidad = cola.size();
            if (profundidad > profundidad_maxima) profundidad_maxima = profundidad;
        }
        consumidor.join();
        fin_ns = now_ns();
    }

    const double segundos = static_cast<double>(fin_ns - inicio_ns) / 1e9;
    py::dict out;
    out["count"] = histograma.count();
    out["min_ns"] = histograma.min();
    out["max_ns"] = histograma.max();
    out["mean_ns"] = histograma.mean();
    out["p50_ns"] = histograma.percentile(0.50);
    out["p90_ns"] = histograma.percentile(0.90);
    out["p99_ns"] = histograma.percentile(0.99);
    out["p999_ns"] = histograma.percentile(0.999);
    out["p9999_ns"] = histograma.percentile(0.9999);
    out["out_of_order"] = fuera_de_orden;
    out["max_queue_depth"] = profundidad_maxima;
    out["full_retries"] = reintentos_llena;
    out["saturated"] = profundidad_maxima >= capacity;
    out["elapsed_s"] = segundos;
    out["throughput_msg_per_s"] = segundos > 0.0 ? static_cast<double>(n_messages) / segundos : 0.0;
    out["padded"] = Padded;
    out["capacity"] = capacity;
    out["target_rate_per_sec"] = target_rate_per_sec;
    return out;
}

}  // namespace

// Envoltura de la cola para pruebas desde Python.
//
// Atencion: operar la cola desde Python **no mide nada util**. El costo de
// cruzar la frontera de pybind11 y adquirir el GIL es de cientos de
// nanosegundos, tres ordenes de magnitud por encima de la operacion que se
// quiere medir. Esta clase existe para verificar la **correccion** --orden FIFO,
// ausencia de perdidas, comportamiento en los limites-- no el rendimiento. Para
// rendimiento esta `benchmark`, que corre los dos hilos enteros en C++.
class PyQueue {
   public:
    explicit PyQueue(std::size_t capacity) : cola_(capacity) {}

    bool push(std::uint64_t sequence, double price, double size, std::int32_t side) {
        Tick t{};
        t.sequence = sequence;
        t.timestamp_ns = now_ns();
        t.price = price;
        t.size = size;
        t.side = side;
        return cola_.try_push(t);
    }

    py::object pop() {
        Tick t{};
        if (!cola_.try_pop(t)) return py::none();
        py::dict d;
        d["sequence"] = t.sequence;
        d["timestamp_ns"] = t.timestamp_ns;
        d["price"] = t.price;
        d["size"] = t.size;
        d["side"] = t.side;
        return d;
    }

    std::size_t size() const { return cola_.size(); }
    std::size_t capacity() const { return cola_.capacity(); }

   private:
    SPSCQueue<true> cola_;
};

PYBIND11_MODULE(_spsc, m) {
    m.doc() = "Ring buffer SPSC wait-free y medicion de latencia en percentiles";

    py::class_<PyQueue>(m, "RingBuffer")
        .def(py::init<std::size_t>(), py::arg("capacity"))
        .def("push", &PyQueue::push, py::arg("sequence"), py::arg("price"), py::arg("size"),
             py::arg("side"), "Encola un tick. Devuelve False si la cola esta llena.")
        .def("pop", &PyQueue::pop, "Extrae un tick, o None si la cola esta vacia.")
        .def_property_readonly("size", &PyQueue::size)
        .def_property_readonly("capacity", &PyQueue::capacity);

    m.def(
        "benchmark",
        [](std::size_t n_messages, std::size_t capacity, double target_rate_per_sec, bool padded,
           bool busy_wait) {
            if (n_messages == 0) throw std::invalid_argument("n_messages debe ser positivo");
            if (target_rate_per_sec < 0.0)
                throw std::invalid_argument("target_rate_per_sec debe ser no negativo");
            return padded
                       ? run_benchmark<true>(n_messages, capacity, target_rate_per_sec, busy_wait)
                       : run_benchmark<false>(n_messages, capacity, target_rate_per_sec, busy_wait);
        },
        py::arg("n_messages") = 1000000, py::arg("capacity") = 4096,
        py::arg("target_rate_per_sec") = 0.0, py::arg("padded") = true,
        py::arg("busy_wait") = true,
        "Productor y consumidor en hilos separados, enteramente en C++.\n\n"
        "target_rate_per_sec = 0 satura la cola y mide THROUGHPUT; la latencia que "
        "devuelve entonces es espera en cola, no costo de traspaso. Una tasa objetivo "
        "por debajo de la capacidad de proceso mantiene la cola superficial y mide "
        "LATENCIA de verdad. Comprobar 'saturated' y 'max_queue_depth' en el resultado.");

    m.def("clock_resolution_ns", &measure_clock_resolution_ns, py::arg("n_samples") = 1000,
          "Resolucion efectiva de steady_clock: minima diferencia no nula entre dos "
          "lecturas. Imprescindible para interpretar los percentiles de latencia: si el "
          "traspaso cuesta menos que la resolucion, no se puede medir con este reloj.");

    m.attr("__cache_line_size__") = kCacheLine;
    m.attr("__tick_size__") = sizeof(Tick);
}
