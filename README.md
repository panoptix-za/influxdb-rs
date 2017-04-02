# influxdb

influxdb provides an asynchronous Rust interface to an [InfluxDB][] database.

This crate supports insertion of strings already in the InfluxDB Line Protocol.
The `influxdb-derive` crate provides convenient serialization of Rust structs
to this format.

[InfluxDB]: https://www.influxdata.com/

## Examples

To serialize a struct into the InfluxDB Line Protocol format, use the
`influxdb-derive` crate's macros as shown below with `MyMeasure`.

Then create an instance of `influxdb::AsyncDb` and add instances of your
struct. Check out the code in the `examples` directory to see how this code
interacts with futures.

```rust
extern crate influxdb;
#[macro_use]
extern crate influxdb_derive;
extern crate tokio_core;

use std::time::SystemTime;
use influxdb::{Measurement, AsyncDb};

// `Measurement` is the trait that `AsyncDb` needs in order to insert
#[derive(Measurement)]
// The default measurement name will be the struct name; this optional
// annotation allows customization of the name sent to InfluxDB.
#[influx(rename = "my_measure")]
struct MyMeasure {
    // Specify which struct fields are InfluxDB tags.
    // Tags must be `String`s or `&str`s.
    #[influx(tag)]
    region: String,
    // Specify which struct fields are InfluxDB fields.
    // Supported types are integers, floats, strings, and booleans.
    // The rename annotation works with struct fields as well.
    #[influx(field, rename = "amount")]
    count: i32,
    // Specify which struct field is the InfluxDB timestamp.
    #[influx(timestamp)]
    when: SystemTime,
    // Struct fields that aren't annotated won't be sent to InfluxDB.
    other: i32,
}

fn main() {
    let mut core = tokio_core::reactor::Core::new()
        .expect("Unable to create reactor core");

    let async_db = AsyncDb::new(
        core.handle(),            // A tokio_core handle
        "http://localhost:8086/", // URL to InfluxDB
        "my_database"             // Name of the database in InfluxDB
    ).expect("Unable to create AsyncDb");

    let now = SystemTime::now();
    let batch = vec![
        MyMeasure { region: String::from("us-east"), count: 3, when: now, other: 0 },
        MyMeasure { region: String::from("us-west"), count: 20, when: now, other: 1 },
    ];

    let insert = async_db.add_data(&batch); // Returns a Future
    core.run(insert).expect("Unable to run future to completion");
}
```

## Running the tests

The tests assume that InfluxDB is running and has been configured to accept
data via UDP.

On macOS, you can install InfluxDB via Homebrew:

```
brew install influxdb
```

Then add the UDP configuration by appending the provided
`tests/influxdb.udp.conf` to the configuration file
`/usr/local/etc/influxdb.conf` to create a local configuration file:

```
cat /usr/local/etc/influxdb.conf tests/influxdb.udp.conf > influxdb.conf
```

And start InfluxDB with that local configuration file:

```
influxd -config influxdb.conf
```

On Linux, one way to accomplish the same setup is to follow the steps in
`.travis.yml`.

Once you have InfluxDB configured and running, run the tests:

```
cargo test
```

## Caveats

- Because InfluxDB acknowledges requests by ending the HTTP session before it
  has actually performed the requested action, occasionally tests may fail.
  Examples include:
  - The database has not been created when an indexing request is sent
  - The data has not been indexed when a query request is sent
- String escaping has not been implemented; attempting to send the following
  characters will result in malformed Line Protocol data being sent:
  - In measurements: commas or spaces
  - In tag keys, tag values, and field keys: commas, equal signs, or spaces
  - In string field values: quotes
- Currently, queries return values as `serde_json::Value`s. This is a leaky
  abstraction, and not all `serde_json::Value`s are possible.
- The UDP insertion interface creates one socket per submission; this should
  reuse the socket.

## Features not currently implemented

- HTTPS/TLS
- InfluxDB Authorization
- Chunked responses

## License

influxdb-rs is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

## Authors

This crate was created by Jake Goulding and Carol (Nichols || Goulding) of
[Integer 32][], sponsored by Stephan Buys of [Panoptix][].

[Integer 32]: http://www.integer32.com/
[Panoptix]: http://www.panoptix.co.za/
