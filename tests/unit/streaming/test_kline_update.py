# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Unit tests for KlineUpdate dataclass (T14).

Covers:
- from_binance_ws() happy path for closed and open candles
- from_binance_ws() type coercions (strings → float, ms → datetime)
- from_historical_row() for backfill use-cases
- Immutability (frozen=True)
- is_closed flag semantics
- Missing required fields raise KeyError
- data_source default and override
- Equality / hashing (slots=True, frozen=True)
"""

from datetime import datetime, timezone

import pytest

from ckvd.core.streaming.kline_update import KlineUpdate


class TestFromBinanceWsClosed:
    """from_binance_ws() with a closed candle (k.x=True)."""

    def test_symbol_parsed(self, closed_kline_update):
        assert closed_kline_update.symbol == "BTCUSDT"

    def test_interval_parsed(self, closed_kline_update):
        assert closed_kline_update.interval == "1h"

    def test_open_time_is_utc_datetime(self, closed_kline_update):
        assert isinstance(closed_kline_update.open_time, datetime)
        assert closed_kline_update.open_time.tzinfo == timezone.utc

    def test_open_time_correct_value(self, closed_kline_update):
        expected = datetime.fromtimestamp(1_700_000_000_000 / 1000, tz=timezone.utc)
        assert closed_kline_update.open_time == expected

    def test_close_time_is_utc_datetime(self, closed_kline_update):
        assert isinstance(closed_kline_update.close_time, datetime)
        assert closed_kline_update.close_time.tzinfo == timezone.utc

    def test_open_is_float(self, closed_kline_update):
        assert isinstance(closed_kline_update.open, float)
        assert closed_kline_update.open == pytest.approx(36500.00)

    def test_high_is_float(self, closed_kline_update):
        assert closed_kline_update.high == pytest.approx(37000.00)

    def test_low_is_float(self, closed_kline_update):
        assert closed_kline_update.low == pytest.approx(36400.00)

    def test_close_is_float(self, closed_kline_update):
        assert closed_kline_update.close == pytest.approx(36800.00)

    def test_volume_is_float(self, closed_kline_update):
        assert closed_kline_update.volume == pytest.approx(1500.00)

    def test_is_closed_true(self, closed_kline_update):
        assert closed_kline_update.is_closed is True

    def test_data_source_default(self, closed_kline_update):
        assert closed_kline_update.data_source == "STREAMING"


class TestFromBinanceWsOpen:
    """from_binance_ws() with an open (mid-candle) payload (k.x=False)."""

    def test_is_closed_false(self, open_kline_update):
        assert open_kline_update.is_closed is False

    def test_symbol_parsed(self, open_kline_update):
        assert open_kline_update.symbol == "ETHUSDT"

    def test_interval_parsed(self, open_kline_update):
        assert open_kline_update.interval == "1m"

    def test_open_time_correct(self, open_kline_update):
        expected = datetime.fromtimestamp(1_700_000_000_000 / 1000, tz=timezone.utc)
        assert open_kline_update.open_time == expected

    def test_close_price_correct(self, open_kline_update):
        assert open_kline_update.close == pytest.approx(2010.00)


class TestFromBinanceWsEdgeCases:
    """Edge cases in from_binance_ws()."""

    def test_missing_k_key_raises_key_error(self):
        with pytest.raises(KeyError):
            KlineUpdate.from_binance_ws({"e": "kline", "E": 123})

    def test_missing_symbol_in_k_raises_key_error(self):
        with pytest.raises(KeyError):
            KlineUpdate.from_binance_ws({"e": "kline", "k": {"i": "1h"}})

    def test_missing_open_price_raises_key_error(self):
        with pytest.raises(KeyError):
            KlineUpdate.from_binance_ws({
                "e": "kline",
                "k": {"s": "BTCUSDT", "i": "1h", "t": 123, "T": 456},
            })

    def test_string_prices_coerced_to_float(self):
        """Binance sends prices as JSON strings; must be float in KlineUpdate."""
        raw = {
            "e": "kline",
            "k": {
                "t": 1_700_000_000_000,
                "T": 1_700_003_599_999,
                "s": "BTCUSDT",
                "i": "1h",
                "o": "36500.12",
                "c": "36800.99",
                "h": "37000.00",
                "l": "36400.01",
                "v": "1500.55",
                "x": True,
            },
        }
        update = KlineUpdate.from_binance_ws(raw)
        assert isinstance(update.open, float)
        assert isinstance(update.close, float)
        assert update.open == pytest.approx(36500.12)

    def test_timestamps_milliseconds_to_seconds(self):
        """open_time must be converted from ms to datetime correctly."""
        ms = 1_700_000_000_000
        raw = {
            "e": "kline",
            "k": {
                "t": ms,
                "T": ms + 3_599_999,
                "s": "XRPUSDT",
                "i": "1h",
                "o": "0.5",
                "c": "0.51",
                "h": "0.52",
                "l": "0.49",
                "v": "1000000",
                "x": False,
            },
        }
        update = KlineUpdate.from_binance_ws(raw)
        expected_dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        assert update.open_time == expected_dt


class TestFromHistoricalRow:
    """from_historical_row() for backfill use-cases.

    from_historical_row expects a pandas Series where row.name is the open_time
    (the Series index, e.g. from df.iterrows() on a CKVD DataFrame).
    """

    @pytest.fixture
    def open_time(self):
        return datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    @pytest.fixture
    def sample_row(self, open_time):
        """Pandas Series with open_time as the index (row.name = open_time)."""
        import pandas as pd

        return pd.Series(
            {
                "open": 42000.0,
                "high": 43000.0,
                "low": 41000.0,
                "close": 42500.0,
                "volume": 100.0,
                "close_time": datetime(2024, 1, 1, 0, 59, 59, tzinfo=timezone.utc),
            },
            name=open_time,  # row.name = open_time (the DataFrame index)
        )

    def test_symbol_set(self, sample_row):
        update = KlineUpdate.from_historical_row(sample_row, "BTCUSDT", "1h")
        assert update.symbol == "BTCUSDT"

    def test_interval_set(self, sample_row):
        update = KlineUpdate.from_historical_row(sample_row, "BTCUSDT", "1h")
        assert update.interval == "1h"

    def test_is_closed_true(self, sample_row):
        """Historical rows are always fully closed."""
        update = KlineUpdate.from_historical_row(sample_row, "BTCUSDT", "1h")
        assert update.is_closed is True

    def test_data_source_defaults_to_rest(self, sample_row):
        """Without _data_source column the default is REST (FCP REST backfill)."""
        update = KlineUpdate.from_historical_row(sample_row, "BTCUSDT", "1h")
        assert update.data_source == "REST"

    def test_data_source_from_column(self, open_time):
        """When _data_source is in the row, it overrides the default."""
        import pandas as pd

        row = pd.Series(
            {
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "close_time": datetime(2024, 1, 1, 0, 59, tzinfo=timezone.utc),
                "_data_source": "CACHE",
            },
            name=open_time,
        )
        update = KlineUpdate.from_historical_row(row, "BTCUSDT", "1h")
        assert update.data_source == "CACHE"

    def test_prices_preserved(self, sample_row):
        update = KlineUpdate.from_historical_row(sample_row, "BTCUSDT", "1h")
        assert update.open == pytest.approx(42000.0)
        assert update.close == pytest.approx(42500.0)

    def test_open_time_preserved(self, sample_row, open_time):
        update = KlineUpdate.from_historical_row(sample_row, "BTCUSDT", "1h")
        assert update.open_time == open_time


class TestKlineUpdateImmutability:
    """KlineUpdate is frozen=True — no mutations allowed."""

    def test_cannot_set_attribute(self, closed_kline_update):
        with pytest.raises((AttributeError, TypeError)):
            closed_kline_update.close = 99999.0  # type: ignore[misc]

    def test_cannot_del_attribute(self, closed_kline_update):
        with pytest.raises((AttributeError, TypeError)):
            del closed_kline_update.symbol  # type: ignore[misc]


class TestKlineUpdateHashAndEquality:
    """KlineUpdate should be hashable and equality-comparable (frozen dataclass)."""

    def test_equal_updates_are_equal(self, raw_kline_closed):
        a = KlineUpdate.from_binance_ws(raw_kline_closed)
        b = KlineUpdate.from_binance_ws(raw_kline_closed)
        assert a == b

    def test_different_updates_not_equal(self, raw_kline_closed, raw_kline_open):
        a = KlineUpdate.from_binance_ws(raw_kline_closed)
        b = KlineUpdate.from_binance_ws(raw_kline_open)
        assert a != b

    def test_hashable(self, closed_kline_update):
        s = {closed_kline_update}
        assert closed_kline_update in s

    def test_usable_as_dict_key(self, closed_kline_update):
        d = {closed_kline_update: "val"}
        assert d[closed_kline_update] == "val"


class TestKlineUpdateDataSource:
    """data_source field defaults and overrides."""

    def test_default_is_streaming(self, raw_kline_closed):
        update = KlineUpdate.from_binance_ws(raw_kline_closed)
        assert update.data_source == "STREAMING"

    def test_historical_row_data_source_defaults_rest(self):
        """from_historical_row defaults data_source to 'REST' (FCP REST backfill)."""
        import pandas as pd

        row = pd.Series(
            {
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "close_time": datetime(2024, 1, 1, 0, 59, tzinfo=timezone.utc),
            },
            name=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        update = KlineUpdate.from_historical_row(row, "BTCUSDT", "1h")
        assert update.data_source == "REST"
