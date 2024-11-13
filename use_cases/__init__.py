# Instead of aliasing, import the functions directly
from .use_case_1 import run_use_case as run_use_case_1
from .use_case_2 import run_use_case as run_use_case_2

__all__ = ["run_use_case_1", "run_use_case_2"]
