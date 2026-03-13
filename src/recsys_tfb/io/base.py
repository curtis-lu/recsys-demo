from abc import ABC, abstractmethod


class AbstractDataset(ABC):
    """Abstract base class for all dataset implementations."""

    @abstractmethod
    def load(self):
        """Load data from the dataset."""
        ...

    @abstractmethod
    def save(self, data) -> None:
        """Save data to the dataset."""
        ...

    @abstractmethod
    def exists(self) -> bool:
        """Check if the dataset exists."""
        ...
