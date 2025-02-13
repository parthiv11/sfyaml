from setuptools import setup, find_packages

setup(
    name="sfyaml",
    version="0.1.0",
    author="Parthiv Makwana",
    author_email="parthivmakwana11@gmail.com",
    description="A CLI tool for managing Snowflake objects using YAML configurations.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/parthiv11/sfyaml",
    packages=find_packages(),
    install_requires=[
        "pyyaml",
        "snowflake-connector-python",
        "dbt-core",
        "airbyte-api-client",
        "click"
    ],
    entry_points={
        "console_scripts": [
            "sfyaml=sfyaml.cli:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
