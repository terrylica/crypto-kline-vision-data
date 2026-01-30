"""Core data source management functionality."""

from .sync.data_source_manager import DataSourceManager, DataSource, DataSourceConfig

__all__ = ["DataSource", "DataSourceConfig", "DataSourceManager"]