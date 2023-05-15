# Installing dependencies

## Creating a virtualenv

Although not required, it's recommended to create a virtualenv prior to installing this library and
its dependencies.

```bash
# create virtualenv
python3 -m venv venv
# activate virtualenv
. venv/bin/activate
```

## Installing this library

This library can be installed using the following command:

```bash
pip install -e 'git+https://git@github.com/splitgraph/seafowl.git@main#egg=seafowl&subdirectory=examples/clients/python'
```

The above command will _not_ automatically install dependencies required to use Seafowl with pandas.
To do so, use:

```bash
pip install -e 'git+https://git@github.com/splitgraph/seafowl.git@main#egg=seafowl[pandas]&subdirectory=examples/clients/python'
```

## Dependencies for development

Typechecking requires `mypy` and the typings for libraries to be installed in addition to the
`seafowl` library itself

```bash
pip install mypy black
mypy --install-types
```

# CLI usage

Similar to the nodejs commandline client, the python client also expects the _full_ path including
the `/q` suffix in the `ENDPOINT` environment variable. To query a different database than
`default`, update the endpoint, eg: `http://seafowl-instance.acme.com:80/mydb/q` to query `mydb`.

```bash
PASSWORD="my-super-secret-password" ENDPOINT="http://seafowl-instance.acme.com:80/q" python -m seafowl 'SELECT * FROM "public"."nba" LIMIT 10'
```

# Library usage examples

## Querying Seafowl over HTTP

```python
import os
from seafowl import query, SeafowlConnectionParams

conn = SeafowlConnectionParams(
    url=os.environ['ENDPOINT'],
    secret=os.environ['PASSWORD'],
    database=None
)

import pprint
pprint.pprint(query(conn, "SELECT 1"))
```

To run this script, invoke it as:

```bash
PASSWORD="my-super-secret-password" ENDPOINT="http://seafowl-instance.acme.com:80"  python script.py
```

## Using Seafowl with pandas DataFrames

Loading `pandas` `DataFrame`s into Seafowl:

```python
import pandas as pd
import os
from seafowl import query, SeafowlConnectionParams
from seafowl.types import QualifiedTableName
from seafowl.dataframe_to_sql import dataframe_to_seafowl

# load a CSV file into pandas
data = pd.read_csv("https://media.geeksforgeeks.org/wp-content/uploads/nba.csv")

conn = SeafowlConnectionParams(
    url=os.environ['ENDPOINT'],
    secret=os.environ['PASSWORD'],
    database=None
)

destination = QualifiedTableName(schema="public", table=os.environ['TABLE'])

dataframe_to_seafowl(data, conn, destination)
```

Assuming the python-seafowl-client and its dependencies are installed, the above script can be run
as (note: no trailing `/q` required here):

```bash
PASSWORD="my-super-secret-password" ENDPOINT="http://seafowl-instance.acme.com:80" TABLE="nba2" python pandas-example.py
```
