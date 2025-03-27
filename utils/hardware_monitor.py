#!/usr/bin/env python

import os
import psutil
import asyncio
import aiohttp
from typing import Dict, Optional, List, Coroutine, Any
from dataclasses import dataclass
from utils.network_utils import create_client

from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False)


@dataclass
class HardwareMetrics:
    """Hardware metrics for resource monitoring."""

    cpu_count: int
    memory_available_gb: float
    network_bandwidth_mbps: Optional[float] = None
    iowait_percent: float = 0.0


class HardwareMonitor:
    """Monitor hardware resources and optimize for maximum Binance API utilization."""

    def __init__(self):
        self._metrics: Optional[HardwareMetrics] = None
        self._last_update = 0
        self._update_interval = 1  # 1 second updates for responsive scaling
        self._bandwidth_requirement = (
            0.05  # Reduced to 0.05MB per connection (more realistic)
        )
        self._binance_rate_limit = 1200  # Binance's rate limit per minute
        self._endpoints = [
            "https://api.binance.com",
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
            "https://api4.binance.com",
            "https://api-gcp.binance.com",
        ]

    async def measure_network_speed(self, test_url: Optional[str] = None) -> float:
        """Measure network bandwidth using parallel requests across multiple endpoints.

        Makes concurrent requests to multiple Binance endpoints for better bandwidth estimation.
        """
        try:
            bandwidths = []
            # Test multiple endpoints in parallel
            async with create_client(
                client_type="aiohttp",
                timeout=2.0,
                headers={"User-Agent": "BinanceDataServices/BandwidthTest"},
            ) as session:
                tasks: List[Coroutine[Any, Any, float]] = []
                for endpoint in self._endpoints:
                    for _ in range(2):  # 2 requests per endpoint
                        tasks.append(self._measure_single_endpoint(session, endpoint))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                bandwidths = [r for r in results if isinstance(r, float)]

            if not bandwidths:
                return 50.0  # Default to 50 Mbps if all measurements fail

            # Use 90th percentile for more aggressive scaling
            return sorted(bandwidths)[int(len(bandwidths) * 0.9)]

        except Exception as e:
            logger.warning(f"Failed to measure network speed: {e}")
            return 50.0  # Default to 50 Mbps

    async def _measure_single_endpoint(
        self, session: aiohttp.ClientSession, endpoint: str
    ) -> float:
        """Measure bandwidth for a single endpoint."""
        try:
            start_time = asyncio.get_event_loop().time()
            async with session.get(
                f"{endpoint}/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1s", "limit": 100},
                timeout=2.0,
            ) as response:
                data = await response.read()
                duration = asyncio.get_event_loop().time() - start_time
                size_mb = len(data) / (1024 * 1024)  # Convert to MB
                return (size_mb * 8) / duration  # Convert to Mbps
        except Exception:
            return 0.0

    def get_hardware_metrics(self) -> HardwareMetrics:
        """Get current hardware metrics."""
        cpu_count = os.cpu_count() or 1
        memory = psutil.virtual_memory()
        memory_available_gb = memory.available / (1024 * 1024 * 1024)  # Convert to GB

        # Get CPU IOWait percentage
        cpu_times = psutil.cpu_times_percent()
        iowait = getattr(cpu_times, "iowait", 0.0)

        return HardwareMetrics(
            cpu_count=cpu_count,
            memory_available_gb=memory_available_gb,
            iowait_percent=iowait,
        )

    async def update_metrics(self) -> None:
        """Update hardware metrics including network speed."""
        metrics = self.get_hardware_metrics()
        network_speed = await self.measure_network_speed()
        metrics.network_bandwidth_mbps = network_speed
        self._metrics = metrics
        self._last_update = asyncio.get_event_loop().time()

    def calculate_optimal_concurrency(
        self,
        base_concurrent_requests: int = 20,
        min_concurrent_requests: int = 10,
        max_concurrent_requests: int = 50,  # Increased for better utilization
    ) -> Dict[str, Any]:
        """Calculate optimal number of concurrent requests based on Binance's capabilities.

        Args:
            base_concurrent_requests: Base number of concurrent requests (default: 20)
            min_concurrent_requests: Minimum number of concurrent requests (default: 10)
            max_concurrent_requests: Maximum number of concurrent requests (default: 50)

        Returns:
            Dictionary containing optimal concurrency and the factors considered
        """
        if not self._metrics:
            return {
                "optimal_concurrency": base_concurrent_requests,
                "limiting_factor": "no_metrics",
            }

        # CPU-based concurrency - Use 2x CPU threads for optimal I/O operations
        cpu_optimal = self._metrics.cpu_count * 2

        # Memory-based concurrency - Each connection only needs about 2MB
        memory_connections = int(self._metrics.memory_available_gb * 1024 / 2)
        memory_optimal = min(memory_connections, 100)  # Cap at 100

        # Network-based concurrency
        if self._metrics.network_bandwidth_mbps:
            # More aggressive network utilization
            network_optimal = int(
                self._metrics.network_bandwidth_mbps / (self._bandwidth_requirement * 8)
            )
            # Consider multiple endpoints
            network_optimal *= len(self._endpoints)
        else:
            network_optimal = base_concurrent_requests * len(self._endpoints)

        # Rate limit based concurrency
        # Binance allows 1200 requests per minute = 20 requests per second
        rate_limit_optimal = self._binance_rate_limit // 60  # Requests per second
        rate_limit_optimal *= len(self._endpoints)  # Multiple endpoints

        # I/O wait is less critical for network operations
        iowait_factor = 0.8 if self._metrics.iowait_percent > 50 else 1.0

        # Calculate final concurrency
        optimal = min(
            cpu_optimal,
            memory_optimal,
            network_optimal,
            rate_limit_optimal,
            max_concurrent_requests,
        )
        optimal = max(min_concurrent_requests, int(optimal * iowait_factor))

        return {
            "optimal_concurrency": optimal,
            "limiting_factor": self._determine_limiting_factor(
                optimal,
                cpu_optimal,
                memory_optimal,
                network_optimal,
                rate_limit_optimal,
            ),
            "metrics": {
                "cpu_optimal": cpu_optimal,
                "memory_optimal": memory_optimal,
                "network_optimal": network_optimal,
                "rate_limit_optimal": rate_limit_optimal,
                "iowait_factor": iowait_factor,
                "endpoints_available": len(self._endpoints),
            },
        }

    def _determine_limiting_factor(
        self,
        optimal: int,
        cpu_optimal: int,
        memory_optimal: int,
        network_optimal: int,
        rate_limit_optimal: int,
    ) -> str:
        """Determine which factor is limiting concurrency."""
        factors = {
            "cpu": cpu_optimal,
            "memory": memory_optimal,
            "network": network_optimal,
            "rate_limit": rate_limit_optimal,
        }
        return min(factors.items(), key=lambda x: x[1])[0]

    def get_optimal_concurrency(
        self,
        base_concurrent_requests: int = 20,
        min_concurrent_requests: int = 10,
        max_concurrent_requests: int = 50,
    ) -> Dict[str, Any]:
        """Alias for calculate_optimal_concurrency to maintain backward compatibility.

        Args:
            base_concurrent_requests: Base number of concurrent requests (default: 20)
            min_concurrent_requests: Minimum number of concurrent requests (default: 10)
            max_concurrent_requests: Maximum number of concurrent requests (default: 50)

        Returns:
            Dictionary containing optimal concurrency and the factors considered
        """
        return self.calculate_optimal_concurrency(
            base_concurrent_requests, min_concurrent_requests, max_concurrent_requests
        )
