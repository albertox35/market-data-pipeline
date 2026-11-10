"""Build del nucleo nativo del pipeline."""

from __future__ import annotations

import sys

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup

if sys.platform == "win32":
    extra_compile_args = ["/O2", "/std:c++20"]
    extra_link_args: list[str] = []
else:
    extra_compile_args = ["-O3", "-std=c++20", "-pthread"]
    extra_link_args = ["-pthread"]

ext_modules = [
    Pybind11Extension(
        "mdpipeline._spsc",
        ["cpp/spsc.cpp"],
        cxx_std=20,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    )
]

setup(ext_modules=ext_modules, cmdclass={"build_ext": build_ext})
