"""Reconstruccion del libro de ordenes L2 con recuperacion por hueco de secuencia.

Esta es la parte que define la **calidad** de un pipeline de market data, y la
que la propuesta original no mencionaba. Un feed incremental envia deltas: «el
nivel 100.25 ahora tiene 500 de volumen». El libro se reconstruye aplicandolos en
orden, y de ahi se siguen tres problemas que hay que resolver explicitamente:

**Huecos de secuencia.** Si se pierde un mensaje, todos los deltas posteriores se
aplican sobre un estado incorrecto. El libro no «se degrada»: queda **mal**, y
sigue pareciendo valido. La unica salida correcta es marcarlo como invalido y
reconstruirlo desde un snapshot. Seguir aplicando deltas tras un hueco es el
error mas grave posible en este componente, porque produce precios erroneos sin
ninguna senial.

**Recuperacion por snapshot.** El snapshot llega con su propio numero de
secuencia, y los deltas que llegaron mientras se pedia hay que descartarlos o
aplicarlos segun si son anteriores o posteriores a el.

**Libro cruzado.** Si la mejor compra alcanza o supera la mejor venta, el estado
es incoherente. Puede ser un error de reconstruccion o un problema del feed, pero
en cualquier caso **no es un libro valido** y hay que detectarlo en lugar de
publicar un diferencial negativo.

Convencion: las compras se ordenan de mayor a menor precio y las ventas de menor
a mayor. Un volumen de cero **elimina** el nivel, que es la convencion de la
mayoria de los protocolos.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

__all__ = ["Side", "BookState", "Level", "OrderBook", "GapDetected"]


class Side(str, Enum):
    """Lado del libro."""

    BID = "bid"
    ASK = "ask"


class BookState(str, Enum):
    """Estado de validez del libro."""

    UNINITIALISED = "sin_inicializar"
    """Nunca se recibio un snapshot."""

    VALID = "valido"
    """Secuencia continua desde el ultimo snapshot."""

    STALE = "invalido_por_hueco"
    """Se detecto un hueco de secuencia. **No se puede confiar en el contenido**
    hasta recibir un snapshot nuevo."""


class GapDetected(Exception):
    """Se intento aplicar un delta sobre un libro invalidado por un hueco."""


@dataclass(frozen=True)
class Level:
    """Un nivel del libro."""

    price: float
    size: float


@dataclass
class OrderBook:
    """Libro de ordenes L2 reconstruido a partir de deltas incrementales.

    Parameters
    ----------
    symbol:
        Identificador del instrumento.
    strict:
        Si ``True`` (por omision), aplicar un delta sobre un libro invalidado
        lanza :class:`GapDetected` en lugar de corromper el estado en silencio.
    """

    symbol: str
    strict: bool = True
    state: BookState = BookState.UNINITIALISED
    last_sequence: int = -1
    gap_count: int = 0
    snapshot_count: int = 0
    applied_deltas: int = 0
    discarded_deltas: int = 0
    _bids: dict[float, float] = field(default_factory=dict, repr=False)
    _asks: dict[float, float] = field(default_factory=dict, repr=False)

    # ------------------------------------------------------------- snapshots

    def apply_snapshot(self, sequence: int, bids: list[Level], asks: list[Level]) -> None:
        """Reemplaza el libro completo con un snapshot.

        Es la **unica** via de recuperacion tras un hueco: reconstruir el estado
        desde cero. Aplicar deltas para «ponerse al dia» no funciona, porque los
        deltas perdidos contenian informacion que no esta en ninguna otra parte.
        """
        if sequence < 0:
            raise ValueError("el numero de secuencia debe ser no negativo")
        self._bids = {lv.price: lv.size for lv in bids if lv.size > 0.0}
        self._asks = {lv.price: lv.size for lv in asks if lv.size > 0.0}
        self.last_sequence = int(sequence)
        self.state = BookState.VALID
        self.snapshot_count += 1

    # ----------------------------------------------------------------- deltas

    def apply_delta(self, sequence: int, side: Side, price: float, size: float) -> bool:
        """Aplica un delta incremental.

        Returns
        -------
        bool
            ``True`` si el delta se aplico. ``False`` si se descarto por ser
            anterior al estado actual, lo que ocurre de forma normal tras una
            recuperacion por snapshot.

        Raises
        ------
        GapDetected
            Si hay un salto en la secuencia, o si el libro ya estaba invalidado.
            En modo no estricto se registra el hueco y se devuelve ``False``.
        """
        if size < 0.0:
            raise ValueError("el volumen no puede ser negativo")

        if self.state is BookState.UNINITIALISED:
            return self._reject(
                f"libro sin inicializar: se requiere un snapshot antes del delta {sequence}"
            )
        if self.state is BookState.STALE:
            return self._reject(
                f"libro invalidado por un hueco previo; el delta {sequence} no se aplica "
                "hasta recibir un snapshot nuevo"
            )

        # Deltas anteriores al snapshot: se descartan sin que sea un error. Es el
        # caso normal de los mensajes que estaban en vuelo mientras se pedia el
        # snapshot.
        if sequence <= self.last_sequence:
            self.discarded_deltas += 1
            return False

        if sequence != self.last_sequence + 1:
            self.gap_count += 1
            self.state = BookState.STALE
            return self._reject(
                f"hueco de secuencia: se esperaba {self.last_sequence + 1} y llego "
                f"{sequence}; faltan {sequence - self.last_sequence - 1} mensajes. "
                "El libro queda invalidado hasta el proximo snapshot."
            )

        libro = self._bids if side is Side.BID else self._asks
        if size == 0.0:
            libro.pop(price, None)
        else:
            libro[price] = size
        self.last_sequence = int(sequence)
        self.applied_deltas += 1
        return True

    def _reject(self, mensaje: str) -> bool:
        if self.strict:
            raise GapDetected(mensaje)
        return False

    # ----------------------------------------------------------------- lectura

    @property
    def is_valid(self) -> bool:
        """``True`` si se puede confiar en el contenido del libro."""
        return self.state is BookState.VALID

    @property
    def best_bid(self) -> Level | None:
        """Mejor compra, o ``None`` si no hay."""
        if not self._bids:
            return None
        p = max(self._bids)
        return Level(p, self._bids[p])

    @property
    def best_ask(self) -> Level | None:
        """Mejor venta, o ``None`` si no hay."""
        if not self._asks:
            return None
        p = min(self._asks)
        return Level(p, self._asks[p])

    @property
    def spread(self) -> float | None:
        """Diferencial entre la mejor venta y la mejor compra."""
        b, a = self.best_bid, self.best_ask
        if b is None or a is None:
            return None
        return a.price - b.price

    @property
    def mid(self) -> float | None:
        """Precio medio."""
        b, a = self.best_bid, self.best_ask
        if b is None or a is None:
            return None
        return 0.5 * (a.price + b.price)

    @property
    def microprice(self) -> float | None:
        """Precio medio ponderado por volumen en el mejor nivel.

        .. math:: \\frac{P_b S_a + P_a S_b}{S_a + S_b}

        Pondera **al contrario** de lo que sugiere la intuicion: el precio se
        acerca al lado con **menos** volumen, porque ese es el que se agota
        primero. Es un mejor predictor del siguiente movimiento que el medio
        simple.
        """
        b, a = self.best_bid, self.best_ask
        if b is None or a is None:
            return None
        total = a.size + b.size
        if total <= 0.0:
            return None
        return (b.price * a.size + a.price * b.size) / total

    @property
    def is_crossed(self) -> bool:
        """``True`` si la mejor compra alcanza o supera la mejor venta.

        Un libro cruzado no es un libro valido: puede venir de un error de
        reconstruccion o de un problema del feed, pero publicar un diferencial
        negativo como si fuera un precio es un error en cualquier caso.
        """
        b, a = self.best_bid, self.best_ask
        if b is None or a is None:
            return False
        return b.price >= a.price

    def depth(self, side: Side, n_levels: int = 10) -> list[Level]:
        """``n`` mejores niveles de un lado, ordenados por prioridad de precio."""
        if n_levels < 1:
            raise ValueError("n_levels debe ser al menos 1")
        libro = self._bids if side is Side.BID else self._asks
        precios = sorted(libro, reverse=(side is Side.BID))[:n_levels]
        return [Level(p, libro[p]) for p in precios]

    @property
    def n_levels(self) -> tuple[int, int]:
        """Numero de niveles en cada lado."""
        return len(self._bids), len(self._asks)

    def imbalance(self, n_levels: int = 5) -> float | None:
        """Desequilibrio de volumen en los ``n`` mejores niveles.

        .. math:: \\frac{V_b - V_a}{V_b + V_a}

        Positivo cuando hay mas volumen de compra. Es una de las seniales mas
        usadas de presion direccional de corto plazo.
        """
        vb = sum(lv.size for lv in self.depth(Side.BID, n_levels))
        va = sum(lv.size for lv in self.depth(Side.ASK, n_levels))
        if vb + va <= 0.0:
            return None
        return (vb - va) / (vb + va)

    def stats(self) -> dict[str, float | int | str]:
        """Estadisticas de integridad del feed.

        La tasa de huecos es una metrica de calidad del feed que conviene vigilar
        de forma continua: un aumento suele preceder a problemas mayores.
        """
        total = self.applied_deltas + self.discarded_deltas + self.gap_count
        return {
            "symbol": self.symbol,
            "state": self.state.value,
            "last_sequence": self.last_sequence,
            "applied_deltas": self.applied_deltas,
            "discarded_deltas": self.discarded_deltas,
            "gap_count": self.gap_count,
            "snapshot_count": self.snapshot_count,
            "gap_rate": self.gap_count / total if total else 0.0,
            "n_bid_levels": len(self._bids),
            "n_ask_levels": len(self._asks),
        }
