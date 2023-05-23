from distutils.core import setup

setup(
    name="seafowl",
    version="0.0.1",
    description="Python client for the Seafowl analytical SQL database.",
    license="Apache 2.0",
    author="Splitgraph, Inc",
    author_email="hello@splitgraph.com",
    url="https://github.com/splitgraph/seafowl/tree/main/examples/clients/python",
    packages=["seafowl"],
    install_requires=["requests"],
    extras_require={
        "pandas": ["pandas", "SQLAlchemy"],
        "parquet": ["pyarrow"]
      },
)
