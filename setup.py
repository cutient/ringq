# Copyright (c) 2026 cutient
# SPDX-License-Identifier: MIT

from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension("ringq._core", ["src/ringq/_core.pyx"]),
    Extension("ringq._fast_validate", ["src/ringq/_fast_validate.pyx"]),
]

setup(
    ext_modules=cythonize(extensions, language_level="3"),
    package_dir={"": "src"},
)
