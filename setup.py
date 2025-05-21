
from setuptools import setup, find_packages

setup(
    name="timeplus-messaging",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "proton-driver",
    ],
    python_requires=">=3.7",
)