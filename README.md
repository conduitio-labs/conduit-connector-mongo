# Conduit Connector MongoDB

The [MongoDB](https://www.mongodb.com/) connector is one of Conduit plugins. It
provides both, a source and a destination MongoDB connector.

<!-- readmegen:description -->
## Source

The MongoDB Source Connector connects to a MongoDB with the provided `uri`, `db`
and `collection` and starts creating records for each change detected in a
collection.

Upon starting, the Source takes a snapshot of a given collection in the
database, then switches into CDC mode. In CDC mode, the plugin reads events from
a [Change Stream](https://www.mongodb.com/docs/manual/changeStreams/). In order
for this to work correctly, your MongoDB instance must
meet [the criteria](https://www.mongodb.com/docs/manual/changeStreams/#availability)
specified on the official website.

### Snapshot Capture

When the connector first starts, snapshot mode is enabled. The connector reads
all rows of a collection in batches using
a [cursor-based](https://www.mongodb.com/docs/drivers/go/current/fundamentals/crud/read-operations/cursor/)
pagination,
limiting the rows by `batchSize`. The connector stores the last processed
element value of an `orderingColumn` in a position, so the snapshot process can
be paused and resumed without losing data. Once all rows in that initial
snapshot are read the connector switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding
`"snapshot": false` to the Source configuration.

### Change Data Capture

The connector implements CDC features for MongoDB by using a Change Stream that
listens to changes in the configured collection. Every detected change is
converted into a record and returned in the call to `Read`. If there is no
available record when `Read` is called, the connector returns
`sdk.ErrBackoffRetry` error.

The connector stores a `resumeToken` of every Change Stream event in a position,
so the CDC process is resumble.

> **Warning**
>
> [Azure CosmosDB for MongoDB](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams)
> has very limited support for Change Streams, so they cannot be used for CDC.
> If CDC is not possible, like in the case with CosmosDB, the connector only
> supports detecting insert operations by polling for new documents.

### Key handling

The connector always uses the `_id` field as a key.

If the `_id` field is `bson.ObjectID` the connector converts it to a string when
transferring a record to a destination, otherwise, it leaves it unchanged.

## Destination

The MongoDB Destination takes a `opencdc.Record` and parses it into a valid
MongoDB query. The Destination is designed to handle different payloads and
keys. Because of this, each record is individually parsed and written.

### Collection name

If a record contains an `opencdc.collection` property in its metadata it will be
written in that collection, otherwise it will fall back to use the `collection`
configured in the connector. Thus, a Destination can support multiple
collections in the same connector, as long as the user has proper access to
those collections.

### Key handling

The connector uses all keys from an `opencdc.Record` when updating and deleting
documents.

If the `_id` field can be converted to a `bson.ObjectID`, the connector converts
it, otherwise, it uses it as it is.<!-- /readmegen:description -->

### Source Configuration Parameters

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "mongo"
        settings:
          # Collection is the name of a collection the connector must write to
          # (destination) or read from (source).
          # Type: string
          # Required: yes
          collection: ""
          # DB is the name of a database the connector must work with.
          # Type: string
          # Required: yes
          db: ""
          # AWSSessionToken is an AWS session token.
          # Type: string
          # Required: no
          auth.awsSessionToken: ""
          # DB is the name of a database that contains the user's authentication
          # data.
          # Type: string
          # Required: no
          auth.db: ""
          # Mechanism is the authentication mechanism.
          # Type: string
          # Required: no
          auth.mechanism: ""
          # Password is the user's password.
          # Type: string
          # Required: no
          auth.password: ""
          # TLSCAFile is the path to either a single or a bundle of certificate
          # authorities to trust when making a TLS connection.
          # Type: string
          # Required: no
          auth.tls.caFile: ""
          # TLSCertificateKeyFile is the path to the client certificate file or
          # the client private key file.
          # Type: string
          # Required: no
          auth.tls.certificateKeyFile: ""
          # Username is the username.
          # Type: string
          # Required: no
          auth.username: ""
          # BatchSize is the size of a document batch.
          # Type: int
          # Required: no
          batchSize: "1000"
          # OrderingField is the name of a field that is used for ordering
          # collection documents when capturing a snapshot.
          # Type: string
          # Required: no
          orderingField: "_id"
          # Snapshot determines whether the connector will take a snapshot of
          # the entire collection before starting CDC mode.
          # Type: bool
          # Required: no
          snapshot: "true"
          # URI is the connection string. The URI can contain host names,
          # IPv4/IPv6 literals, or an SRV record.
          # Type: string
          # Required: no
          uri: "mongodb://localhost:27017"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

### Configuration

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "mongo"
        settings:
          # Collection is the name of a collection the connector must write to
          # (destination) or read from (source).
          # Type: string
          # Required: yes
          collection: ""
          # DB is the name of a database the connector must work with.
          # Type: string
          # Required: yes
          db: ""
          # AWSSessionToken is an AWS session token.
          # Type: string
          # Required: no
          auth.awsSessionToken: ""
          # DB is the name of a database that contains the user's authentication
          # data.
          # Type: string
          # Required: no
          auth.db: ""
          # Mechanism is the authentication mechanism.
          # Type: string
          # Required: no
          auth.mechanism: ""
          # Password is the user's password.
          # Type: string
          # Required: no
          auth.password: ""
          # TLSCAFile is the path to either a single or a bundle of certificate
          # authorities to trust when making a TLS connection.
          # Type: string
          # Required: no
          auth.tls.caFile: ""
          # TLSCertificateKeyFile is the path to the client certificate file or
          # the client private key file.
          # Type: string
          # Required: no
          auth.tls.certificateKeyFile: ""
          # Username is the username.
          # Type: string
          # Required: no
          auth.username: ""
          # URI is the connection string. The URI can contain host names,
          # IPv4/IPv6 literals, or an SRV record.
          # Type: string
          # Required: no
          uri: "mongodb://localhost:27017"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

### How to build it

Run `make build`.

### Development

Run `make install-tools` to install all the required tools.

Run `make test` to run all the units and `make test-integration` to run all the
integration tests, which require Docker to be installed and running. The command
will handle starting and stopping docker container for you.

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=528a9760-d573-4524-8f65-74a5e4d402e8)
