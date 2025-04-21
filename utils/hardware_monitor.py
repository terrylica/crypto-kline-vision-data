#!/usr/bin/env python
"""Hardware monitoring for optimizing network requests.

This module provides utilities for measuring system resources and network performance
to adjust concurrency parameters for optimal data retrieval.
"""

import os
import time
import psutil
import requests
from dataclasses import dataclass
from typing import Optional, Dict, Any

from utils.logger_setup import logger


@dataclass
class HardwareMetrics:
    """Hardware metrics for resource monitoring."""

    cpu_count: int
    memory_available_gb: float
    network_bandwidth_mbps: Optional[float] = None
    iowait_percent: float = 0.0


class HardwareMonitor:
    """Monitors hardware resources and network performance."""

    def __init__(self):
        """Initialize hardware monitor with default settings."""
        # Use multiple endpoints for network tests to avoid rate limiting
        self._endpoints = [
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
        ]
        self._metrics: Optional[HardwareMetrics] = None
        self._last_update = 0
        self._bandwidth_requirement = 0.5  # MB per request
        self._binance_rate_limit = 1200  # Requests per minute

    def measure_network_speed(self) -> float:
        """Measure network speed to Binance API endpoints.

        Returns:
            Network speed in Mbps
        """
        try:
            bandwidths = []
            # Test multiple endpoints
            for endpoint in self._endpoints:
                for _ in range(2):  # 2 requests per endpoint
                    bandwidth = self._measure_single_endpoint(endpoint)
                    if bandwidth > 0:
                        bandwidths.append(bandwidth)

            if not bandwidths:
                return 50.0  # Default to 50 Mbps if all measurements fail

            # Use 90th percentile for more aggressive scaling
            return sorted(bandwidths)[int(len(bandwidths) * 0.9)]

        except Exception as e:
            logger.warning(f"Failed to measure network speed: {e}")
            return 50.0  # Default to 50 Mbps

    def _measure_single_endpoint(self, endpoint: str) -> float:
        """Measure bandwidth for a single endpoint using requests."""
        try:
            start_time = time.time()

            response = requests.get(
                f"{endpoint}/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1s", "limit": 100},
                timeout=2.0,
                headers={"User-Agent": "BinanceDataServices/BandwidthTest"},
            )
            data = response.content

            duration = time.time() - start_time
            size_mb = len(data) / (1024 * 1024)  # Convert to MB
            return (size_mb * 8) / duration  # Convert to Mbps
        except Exception as e:
            logger.debug(f"Failed to measure endpoint {endpoint}: {e}")
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

    def update_metrics(self) -> None:
        """Update hardware metrics including network speed."""
        metrics = self.get_hardware_metrics()
        network_speed = self.measure_network_speed()
        metrics.network_bandwidth_mbps = network_speed
        self._metrics = metrics
        self._last_update = time.time()

    # Legacy async methods maintained for backward compatibility
    async def measure_network_speed(self) -> float:
        """Async version of measure_network_speed for backward compatibility.

        DEPRECATED: Use the synchronous version instead.
        """
        logger.warning(
            "Async measure_network_speed is deprecated. Use synchronous version instead."
        )
        return self.measure_network_speed()

    async def _measure_single_endpoint(self, session, endpoint: str) -> float:
        """Async version of _measure_single_endpoint for backward compatibility.

        DEPRECATED: Use the synchronous version instead.
        """
        logger.warning(
            "Async _measure_single_endpoint is deprecated. Use synchronous version instead."
        )
        return self._measure_single_endpoint(endpoint)

    async def update_metrics(self) -> None:
        """Async version of update_metrics for backward compatibility.

        DEPRECATED: Use the synchronous version instead.
        """
        logger.warning(
            "Async update_metrics is deprecated. Use synchronous version instead."
        )
        self.update_metrics()

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
