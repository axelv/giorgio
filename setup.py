from setuptools import setup, find_packages

classifiers = [
    "Development Status :: 2 - Pre-Alpha",
    "Intended Audience :: Healthcare Industry",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
]
setup(
    name="giorgio",
    version="0.0.1",
    description="Typesafe Idempotent Data Pipelines",
    long_description=open("README.md").read() + "\n\n",
    url="",
    author="Tiro.health",
    author_email="axel.vanraes@tiro.health",
    license="MIT",
    classifiers=classifiers,
    keywords="",
    packages=find_packages(),
)
