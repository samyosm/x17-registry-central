from x17_registry.domain.errors import IntegrationUnavailable


class ZeroMQBroadcastSource:
    def __init__(self) -> None:
        raise IntegrationUnavailable(
            "Broadcast capture is no longer part of the pull-based registry."
        )
