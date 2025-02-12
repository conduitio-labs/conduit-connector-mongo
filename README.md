# Conduit Connector MongoDB

## General

The [MongoDB](https://www.mongodb.com/) connector is one of Conduit plugins. It
provides both, a source and a destination MongoDB connector.

### Prerequisites

- [Go](https://go.dev/) 1.23+
- [MongoDB](https://www.mongodb.com/) [replica set](https://www.mongodb.com/docs/manual/replication/) (
  at least single-node)
  or [sharded cluster](https://www.mongodb.com/docs/manual/sharding/)
  with [WiredTiger](https://www.mongodb.com/docs/manual/core/wiredtiger/)
  storage engine
- [Docker](https://www.docker.com/)
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) v1.55.2

### How to build it

Run `make build`.

### Development

Run `make install-tools` to install all the required tools.

Run `make test` to run all the units and `make test-integration` to run all the
integration tests, which require Docker to be installed and running. The command
will handle starting and stopping docker container for you.

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
          auth.auth.awsSessionToken: ""
          # DB is the name of a database that contains the user's authentication
          # data.
          # Type: string
          # Required: no
          auth.auth.db: ""
          # Mechanism is the authentication mechanism.
          # Type: string
          # Required: no
          auth.auth.mechanism: ""
          # Password is the user's password.
          # Type: string
          # Required: no
          auth.auth.password: ""
          # TLSCAFile is the path to either a single or a bundle of certificate
          # authorities to trust when making a TLS connection.
          # Type: string
          # Required: no
          auth.auth.tls.caFile: ""
          # TLSCertificateKeyFile is the path to the client certificate file or
          # the client private key file.
          # Type: string
          # Required: no
          auth.auth.tls.certificateKeyFile: ""
          # Username is the username.
          # Type: string
          # Required: no
          auth.auth.username: ""
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
          sdk.schema.extract.key.enabled: "false"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "false"
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
          auth.auth.awsSessionToken: ""
          # DB is the name of a database that contains the user's authentication
          # data.
          # Type: string
          # Required: no
          auth.auth.db: ""
          # Mechanism is the authentication mechanism.
          # Type: string
          # Required: no
          auth.auth.mechanism: ""
          # Password is the user's password.
          # Type: string
          # Required: no
          auth.auth.password: ""
          # TLSCAFile is the path to either a single or a bundle of certificate
          # authorities to trust when making a TLS connection.
          # Type: string
          # Required: no
          auth.auth.tls.caFile: ""
          # TLSCertificateKeyFile is the path to the client certificate file or
          # the client private key file.
          # Type: string
          # Required: no
          auth.auth.tls.certificateKeyFile: ""
          # Username is the username.
          # Type: string
          # Required: no
          auth.auth.username: ""
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

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=528a9760-d573-4524-8f65-74a5e4d402e8)
