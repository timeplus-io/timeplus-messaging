"""
Configuration file for pytest
"""

import pytest
from unittest.mock import MagicMock, patch

# Set up any fixtures that might be needed across multiple test files here
@pytest.fixture
def mock_client():
    """Create a mock Timeplus client for testing"""
    with patch("timeplus_messaging.producer.client.Client") as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance