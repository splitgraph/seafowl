
# Installing dependencies

## Creating a virtualenv
Although not required, it's recommended to create a virtualenv prior to installing `python-seafowl-client` and dependencies.
```bash
# create virtualenv
python3 -m venv venv
# activate virtualenv
. venv/bin/activate
```

## Minimal dependencies
Querying Seafowl requires the `requests` library, which can be installed with
```bash
pip install requests
```

## Dependencies for use with `pandas`
For exporting pandas DataFrames to Seafowl, the following installs all required dependencies
```bash
pip install requests pandas SQLAlchemy
```

## Dependencies for development
Typechecking requires `mypy` and the typings for libraries to be installed
```bash
pip install requests pandas SQLAlchemy mypy
mypy --install-types
```
