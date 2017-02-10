// This example assumes you have InfluxDB running at http://localhost:8086/
// without authentication and that there is an existing database named
// `my_database`.

extern crate tokio_core;
extern crate futures;
extern crate influxdb;

#[macro_use]
extern crate influxdb_derive;

use influxdb::AsyncDb;

const HTTP_BASE_URL: &'static str = "http://localhost:8086/";

#[derive(Measurement)]
#[influx(rename = "my_measure")]
struct MyMeasure {
    #[influx(tag)]
    region: String,
    #[influx(field)]
    count: i32,
}

fn main() {
    let mut core = tokio_core::reactor::Core::new()
        .expect("Unable to create reactor core");

    // The name of the database in InfluxDB
    let db_name = "my_database";

    // Create an influxdb::AsyncDb instance pointing to your InfluxDB
    // instance.
    let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, db_name)
        .expect("Unable to create AsyncDb");

    // influxdb supports batched insertion of multiple records in one request.
    let batch = vec![
        MyMeasure { region: String::from("us-east"), count: 3 },
        MyMeasure { region: String::from("us-west"), count: 20 },
        MyMeasure { region: String::from("us-north"), count: 45 },
    ];

    let insert = async_db.add_data(&batch);

    core.run(insert).expect("Unable to run future to completion");

    let query = async_db.query(r#"SELECT "count" FROM "my_measure""#);

    let response = core.run(query).expect("Unable to run future to completion");

    println!("Values: {:#?}", response.results[0].series[0]);
}
