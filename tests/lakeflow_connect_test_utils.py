from typing import Dict


class LakeflowConnectTestUtils:
    """
    Base class for connector-specific test utilities.
    Each connector should extend this class to provide connector-specific implementations
    for testing operations.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize test utilities with connection options.

        Args:
            options: A dictionary of connection parameters (tokens, credentials, etc.)
                    Same options format as used by LakeflowConnect
        """
        self.options = options
