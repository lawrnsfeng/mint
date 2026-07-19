"""Shared exception infrastructure for all mint packages."""

from dataclasses import dataclass, field
from typing import ClassVar


@dataclass
class TemplatedError(Exception):
    """Base class for dataclass-style exceptions with templated messages.

    Subclasses define fields + TEMPLATE.
    """

    TEMPLATE: ClassVar[str]
    message: str = field(init=False)

    def __post_init__(self) -> None:
        values = {
            name: getattr(self, name) for name in self.__dataclass_fields__ if name != "message"
        }
        self.message = self.TEMPLATE.format(**values)
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message
