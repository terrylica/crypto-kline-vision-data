import pytest


@pytest.fixture(scope="function")
def instrument():
    """
    Provide the trading instrument for OKX tests.
    Returns a default value of BTC-USDT for spot markets.
    """
    return "BTC-USDT"


@pytest.fixture(scope="function")
def interval():
    """
    Provide the trading interval for OKX tests.
    Returns a default value of 1m (1 minute).
    """
    return "1m"