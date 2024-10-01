# fsstratify


## Installation
The following sections give a brief overview of how to install fsstratify and its dependencies.

fsstratify requires at least Python 3.9.

If you want to install fsstratify from this git repository simply run:

```console
$ pip install .
```

If you want to run the tests or if you want to contribute code to fsstratify, you can add the `tests` and `dev` extras to this command:

```console
$ pip install ".[tests]"  # this installs only the dependencies to run the tests
$ pip install ".[dev]"    # this installs additonal dev dependencies and everything from tests
$ pip install ".[all]"    # this installs all dependecies for all possible use cases
```

## Documentation
The documentation is hosted at Read the Docs (TODO add link). If you want to build and view the
documentation locally, you can use the Makefile:

```console
$ make docs
$ make serve-docs
```

The `docs` target build the documentation and the `serve-docs` target starts a local web server
so that you can browse the documentation.

