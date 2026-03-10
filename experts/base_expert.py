from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseExpert(ABC):
    @abstractmethod
    def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError