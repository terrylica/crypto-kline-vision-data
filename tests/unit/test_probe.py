"""Tests for __probe__.py API introspection module.

GitHub Issue #22: Add __probe__.py for AI agent API introspection.
Cross-repo analysis of gapless-crypto-data revealed the probe pattern
enables stateless, deterministic agent discovery of package capabilities.
"""

import json

import pytest


class TestDiscoverApiReturnsDict:
    """Verify discover_api() returns a well-formed dict."""

    def test_returns_dict(self):
        """discover_api() must return a dict."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert isinstance(result, dict)

    def test_json_serializable(self):
        """Output must be JSON-serializable (no non-serializable objects)."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        serialized = json.dumps(result)
        assert isinstance(serialized, str)
        assert len(serialized) > 100  # Non-trivial output

    def test_has_metadata(self):
        """Must contain metadata section."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert "metadata" in result
        assert result["metadata"]["package"] == "crypto-kline-vision-data"
        assert result["metadata"]["import_name"] == "ckvd"

    def test_includes_main_class(self):
        """CryptoKlineVisionData must be present with methods."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert "CryptoKlineVisionData" in result["classes"]
        cls_info = result["classes"]["CryptoKlineVisionData"]
        assert "methods" in cls_info
        assert "get_data" in cls_info["methods"]
        assert "create" in cls_info["methods"]
        assert "close" in cls_info["methods"]

    def test_includes_ckv_config(self):
        """CKVDConfig must be present."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert "CKVDConfig" in result["classes"]


class TestDiscoverApiEnums:
    """Verify discover_api() includes all public enums."""

    @pytest.mark.parametrize(
        "enum_name",
        ["DataProvider", "MarketType", "Interval", "ChartType", "DataSource"],
    )
    def test_enum_present(self, enum_name):
        """Each public enum must be in the enums section."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert enum_name in result["enums"]
        assert "members" in result["enums"][enum_name]
        assert len(result["enums"][enum_name]["members"]) > 0


class TestDiscoverApiFunctions:
    """Verify discover_api() includes public functions."""

    def test_fetch_market_data_present(self):
        """fetch_market_data must be present with parameter info."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert "fetch_market_data" in result["functions"]
        func_info = result["functions"]["fetch_market_data"]
        assert "parameters" in func_info
        assert len(func_info["parameters"]) > 0


class TestDiscoverApiExceptions:
    """Verify discover_api() documents exception hierarchy."""

    def test_exceptions_section_exists(self):
        """Exceptions section must be present."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert "exceptions" in result

    @pytest.mark.parametrize(
        "exc_name",
        ["RestAPIError", "RateLimitError", "VisionAPIError", "DataNotAvailableError"],
    )
    def test_key_exceptions_present(self, exc_name):
        """Key exception classes must be documented."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        assert exc_name in result["exceptions"]

    def test_exceptions_have_details_flag(self):
        """All documented exceptions must indicate .details support."""
        from ckvd.__probe__ import discover_api

        result = discover_api()
        for exc_name, exc_info in result["exceptions"].items():
            assert exc_info["has_details"] is True, f"{exc_name} missing has_details"


class TestGetCapabilities:
    """Verify get_capabilities() returns capability matrix."""

    def test_returns_dict(self):
        """get_capabilities() must return a dict."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert isinstance(result, dict)

    def test_json_serializable(self):
        """Output must be JSON-serializable."""
        from ckvd.__probe__ import get_capabilities

        serialized = json.dumps(get_capabilities())
        assert isinstance(serialized, str)

    def test_providers(self):
        """Must list all DataProvider members."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert "providers" in result
        assert "BINANCE" in result["providers"]

    def test_market_types(self):
        """Must list all MarketType members."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert "market_types" in result
        assert "SPOT" in result["market_types"]
        assert "FUTURES_USDT" in result["market_types"]
        assert "FUTURES_COIN" in result["market_types"]

    def test_intervals(self):
        """Must list all Interval values."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert "intervals" in result
        assert "1h" in result["intervals"]
        assert "1m" in result["intervals"]
        assert "1d" in result["intervals"]

    def test_output_formats(self):
        """Must document both pandas and polars output options."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert "output_formats" in result
        assert "pandas" in result["output_formats"]["default"].lower()
        assert "polars" in result["output_formats"]["opt_in"].lower()

    def test_fcp_section(self):
        """Must document FCP priority order."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert "fcp" in result
        assert result["fcp"]["priority"] == ["CACHE", "VISION", "REST"]

    def test_data_sources(self):
        """Must list all data sources."""
        from ckvd.__probe__ import get_capabilities

        result = get_capabilities()
        assert result["data_sources"] == ["CACHE", "VISION", "REST"]


class TestProbeImport:
    """Verify probe is accessible via lazy import."""

    def test_lazy_import(self):
        """from ckvd import __probe__ must work via lazy import."""
        import importlib

        # Force fresh import
        ckvd = importlib.import_module("ckvd")
        probe = ckvd.__probe__
        assert hasattr(probe, "discover_api")
        assert hasattr(probe, "get_capabilities")

    def test_direct_import(self):
        """from ckvd.__probe__ import discover_api must work."""
        from ckvd.__probe__ import discover_api

        assert callable(discover_api)


class TestProbeIsStateless:
    """Verify probe operations have no side effects."""

    def test_repeated_calls_identical(self):
        """Multiple calls must return identical results (no state mutation)."""
        from ckvd.__probe__ import discover_api, get_capabilities

        api1 = discover_api()
        api2 = discover_api()
        assert json.dumps(api1, sort_keys=True) == json.dumps(api2, sort_keys=True)

        caps1 = get_capabilities()
        caps2 = get_capabilities()
        assert json.dumps(caps1, sort_keys=True) == json.dumps(caps2, sort_keys=True)
