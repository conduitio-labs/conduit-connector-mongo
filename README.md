# Conduit Connector MongoDB

## General

The [MongoDB](https://www.mongodb.com/) connector is one of Conduit plugins. It provides both, a source and a destination MongoDB connector.

### Prerequisites

- [Go](https://go.dev/) 1.18+
- [MongoDB](https://www.mongodb.com/) [replica set](https://www.mongodb.com/docs/manual/replication/) (at least single-node) or [sharded cluster](https://www.mongodb.com/docs/manual/sharding/) with [WiredTiger](https://www.mongodb.com/docs/manual/core/wiredtiger/) storage engine
- [Docker](https://www.docker.com/)
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) v1.50.1

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command will handle starting and stopping docker container for you.

## Source

The MongoDB Source Connector connects to a MongoDB with the provided `uri`, `db` and `collection` and starts creating records for each change detected in a collection.

Upon starting, the Source takes a snapshot of a given collection in the database, then switches into CDC mode. In CDC mode, the plugin reads events from a [Change Stream](https://www.mongodb.com/docs/manual/changeStreams/). In order for this to work correctly, your MongoDB instance must meet [the criteria](https://www.mongodb.com/docs/manual/changeStreams/#availability) specified on the official website.

### Snapshot Capture

When the connector first starts, snapshot mode is enabled. The connector reads all rows of a collection in batches using a [cursor-based](https://www.mongodb.com/docs/drivers/go/current/fundamentals/crud/read-operations/cursor/) pagination,
limiting the rows by `batchSize`. The connector stores the last processed element value of an `orderingColumn` in a position, so the snapshot process can be paused and resumed without losing data. Once all rows in that initial snapshot are read the connector switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding `"snapshot": false` to the Source configuration.

### Change Data Capture

The connector implements CDC features for MongoDB by using a Change Stream that listens to changes in the configured collection. Every detected change is converted into a record and returned in the call to `Read`. If there is no available record when `Read` is called, the connector returns `sdk.ErrBackoffRetry` error.

The connector stores a `resumeToken` of every Change Stream event in a position, so the CDC process is resumble.

> **Warning**
>
> [Azure CosmosDB for MongoDB](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams) has very limited support for Change Streams, so they cannot be used for CDC.
> If CDC is not possible, like in the case with CosmosDB, the connector only supports detecting insert operations by polling for new documents.

### Configuration

| name                          | description                                                                                                                         | required | default                                                                                                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`                         | The connection string. The URI can contain host names, IPv4/IPv6 literals, or an SRV record.                                        | false    | `mongodb://localhost:27017`                                                                                                                                |
| `db`                          | The name of a database the connector must work with.                                                                                | **true** |                                                                                                                                                            |
| `collection`                  | The name of a collection the connector must read from.                                                                              | **true** |                                                                                                                                                            |
| `auth.username`               | The username.                                                                                                                       | false    |                                                                                                                                                            |
| `auth.password`               | The user's password.                                                                                                                | false    |                                                                                                                                                            |
| `auth.db`                     | The name of a database that contains the user's authentication data.                                                                | false    | `admin`                                                                                                                                                    |
| `auth.mechanism`              | The authentication mechanism. The available values are `SCRAM-SHA-256`, `SCRAM-SHA-1`, `MONGODB-CR`, `MONGODB-AWS`, `MONGODB-X509`. | false    | The default mechanism that [defined depending on your MongoDB server version](https://www.mongodb.com/docs/drivers/go/current/fundamentals/auth/#default). |
| `auth.tls.caFile`             | The path to either a single or a bundle of certificate authorities to trust when making a TLS connection.                           | false    |                                                                                                                                                            |
| `auth.tls.certificateKeyFile` | The path to the client certificate file or the client private key file.                                                             | false    |                                                                                                                                                            |
| `batchSize`                   | The size of a document batch.                                                                                                       | false    | `1000`                                                                                                                                                     |
| `snapshot`                    | The field determines whether or not the connector will take a snapshot of the entire collection before starting CDC mode.           | false    | `true`                                                                                                                                                     |
| `orderingField`               | The name of a field that is used for ordering collection documents when capturing a snapshot.                                       | false    | `_id`                                                                                                                                                      |

### Key handling

The connector always uses the `_id` field as a key.

If the `_id` field is `bson.ObjectID` the connector converts it to a string when transferring a record to a destination, otherwise, it leaves it unchanged.
