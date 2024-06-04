## WHAT

Clade ("grouping of organisms ... composed of a common ancestor and all its lineal descendants") is
a backronym for **c**atalog of **la**kehouse **de**finitions.

It's purpose it to provide an interface for tracking provenance and facilitate replication of
queryable data assets.

## HOW

The `schema` module provides the remote catalog functionality which enables query engines in the
Lakehouse context to resolve identifiers such as schemas and tables, as well as map them to the
appropriate storage locations in order to execute queries.

The `sync` module presents a basic protocol for replicating data from remote sources, that is
compatible with Arrow Flight.
