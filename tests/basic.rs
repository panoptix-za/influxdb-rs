extern crate tokio_core;
extern crate futures;
extern crate serde;

extern crate reqwest;
#[macro_use]
extern crate lazy_static;

extern crate influxdb;

#[macro_use]
extern crate influxdb_derive;

use std::error::Error;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::thread;
use std::time::{Duration, SystemTime};

use futures::Future;

use influxdb::{AsyncDb, AsyncUdpDb, QueryResponse, InfluxServerError};

const HTTP_BASE_URL: &'static str = "http://localhost:8086/";

const UDP_IP_AND_PORT: &'static str = "127.0.0.1:8089";
const UDP_DB_NAME: &'static str = "influxdb_rs_udp";
const UDP_BATCH_TIMEOUT_MS: u64 = 50;

#[test]
fn query_asynchronously() {
    let db = fresh_db();

    db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000")
        .unwrap();

    let response = with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, &db.name).unwrap();

        async_db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'"#)
    });

    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));
}

#[test]
fn add_data_asynchronously() {
    let db = fresh_db();

    with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, &db.name).unwrap();

        async_db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000")
    });

    let response: QueryResponse = db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'"#).unwrap();

    assert_eq!(response.results[0].error, None);
    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));
}

#[test]
fn add_batched_data_asynchronously() {
    let db = fresh_db();

    with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, &db.name).unwrap();

        async_db.add_data(r#"
cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000
cpu_load_short,host=server02,region=us-east value=0.23
cpu_load_short,host=server03,region=us-west value=0.01 1434055587000000000
rpm,host=server01,region=us-west value=1434"#)
    });

    let response: QueryResponse = db.query(r#"SELECT "value", "host" FROM "cpu_load_short""#).unwrap();

    assert_eq!(response.results[0].error, None);
    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values.len(), 3);
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));
}

#[test]
fn add_data_asynchronously_udp() {
    let test_db = with_udp_db(|async_db| {
        async_db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000")
    });

    thread::sleep(Duration::from_millis(2 * UDP_BATCH_TIMEOUT_MS));

    let response: QueryResponse = test_db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'"#).unwrap();

    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));
}

#[test]
#[ignore] // TODO: handle errors returned in the response instead of status code
fn query_nonexistent_db() {
    let response = with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, "does_not_exist").unwrap();

        async_db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'"#)
    });

    assert_eq!(response.results[0].error, Some(String::from("database not found: does_not_exist")));
}

#[test]
fn multiple_queries() {
    let db = fresh_db();

    db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000")
        .unwrap();
    db.add_data("cpu_load_short,host=server02,region=us-west value=0.8")
        .unwrap();
    db.add_data("cpu_load_short,host=server01,region=us-west value=0.4")
        .unwrap();

    let mut response = with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, &db.name).unwrap();

        async_db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'; SELECT "value" FROM "cpu_load_short" WHERE "host"='server01'"#)
    });

    response.results.sort_by_key(|k| k.statement_id);

    assert_eq!(response.results[0].statement_id, 0);
    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));

    assert_eq!(response.results[1].statement_id, 1);
    assert_eq!(response.results[1].series[0].name, "cpu_load_short");
    assert_eq!(response.results[1].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[1].series[0].values[1][1].as_f64(), Some(0.4));
}

#[test]
fn multiple_queries_to_nonexistent_database() {
    let response = with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, "does_not_exist").unwrap();

        async_db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'; SELECT "value" FROM "cpu_load_short" WHERE "host"='server01'"#)
    });

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].statement_id, 0);
    assert_eq!(response.results[0].series.len(), 0);
    assert_eq!(response.results[0].error, Some(String::from("not executed")));
}

#[derive(Measurement)]
#[influx(rename = "cpu_load_short")]
struct CpuLoadShort {
    // Multiple attributes
    #[influx(tag)]
    #[influx(rename = "host")]
    hostname: &'static str,
    // Multiple values in one attribute
    #[influx(tag, field)]
    region: &'static str,
    #[influx(field)]
    value: f64,
    #[influx(timestamp)]
    when: SystemTime,
}

#[test]
fn typed_insert() {
    let item = CpuLoadShort {
        hostname: "server03",
        region: "us-west",
        value: 0.78,
        when: SystemTime::now(),
    };

    let db = fresh_db();

    with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, &db.name).unwrap();

        async_db.add_data(item)
    });

    let response: QueryResponse = db.query(r#"SELECT "value", "host" FROM "cpu_load_short""#).unwrap();

    assert_eq!(response.results[0].error, None);
    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values.len(), 1);
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.78));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server03"));
}

#[test]
fn batched_typed_insert() {
    let items = vec![
        CpuLoadShort {
            hostname: "server03",
            region: "us-west",
            value: 0.78,
            when: SystemTime::now(),
        },
        CpuLoadShort {
            hostname: "server07",
            region: "us-east",
            value: 0.32,
            when: SystemTime::now(),
        },
        CpuLoadShort {
            hostname: "server05",
            region: "us-west",
            value: 0.55,
            when: SystemTime::now(),
        },
    ];

    let db = fresh_db();

    with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HTTP_BASE_URL, &db.name).unwrap();

        async_db.add_data(&items)
    });

    let response: QueryResponse = db.query(r#"SELECT "value", "host" FROM "cpu_load_short""#).unwrap();

    assert_eq!(response.results[0].error, None);
    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values.len(), 3);
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.78));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server03"));
}

#[test]
fn test_infrastructure() {
    let db = fresh_db();

    db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000")
        .unwrap();

    let response: QueryResponse =
        db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'"#)
        .unwrap();

    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));
}

fn fresh_db() -> TestingDb {
    static DB_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;

    let id = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!("influxdb_rs_{}", id);
    fresh_db_named(name)
}

fn fresh_db_named<S>(name: S) -> TestingDb
    where S: Into<String>,
{
    let db = TestingDb::new(HTTP_BASE_URL, name)
        .expect("Unable to create test database");
    db.create_db()
        .expect("Unable to create test database");

    // The InfluxDB created isn't ready immediately; this prevents
    // intermittent failures.
    thread::sleep(Duration::from_millis(5));
    db
}

// The InfluxDB UDP ingestion matches one UDP port to one
// database. Since we aren't starting a server for each test, and we
// want each test to be isolated, we can only have one UDP test at a
// time.
fn with_udp_db<F, T>(f: F) -> TestingDb
    where F: FnOnce(AsyncUdpDb) -> T,
          T: Future,
          T::Error: std::fmt::Debug,
{
    lazy_static! {
        static ref MUTEX: Mutex<()> = Mutex::new(());
    }

    let _lock = MUTEX.lock().expect("Unable to lock for UDP test");
    let test_db = fresh_db_named(UDP_DB_NAME);

    with_core(|core| {
        let db = AsyncUdpDb::new(core.handle(), UDP_IP_AND_PORT)
            .expect("Unable to create UDP database");
        f(db)
    });

    test_db
}

fn with_core<F, T>(f: F) -> T::Item
    where F: FnOnce(&mut tokio_core::reactor::Core) -> T,
          T: Future,
          T::Error: std::fmt::Debug,
{
    let mut core = tokio_core::reactor::Core::new()
        .expect("Unable to create reactor core for testing");

    let future = f(&mut core);

    core.run(future).expect("Unable to run future to completion")
}

struct TestingDb {
    name: String,
    client: reqwest::Client,
    query_endpoint: reqwest::Url,
    write_endpoint: reqwest::Url,
}

impl TestingDb {
    /// `base_url` should include the protocol, host, and port (`http://localhost:8086`)
    fn new<S>(base_url: &str, name: S) -> Result<Self, Box<Error>>
        where S: Into<String>,
    {
        let name = name.into();

        let base_url = reqwest::Url::parse(base_url)?;
        let query_endpoint = base_url.join("/query")?;
        let mut write_endpoint = base_url.join("/write")?;
        write_endpoint.query_pairs_mut()
            .append_pair("db", &name);

        let client = reqwest::Client::new()?;

        Ok(TestingDb {
            name: name,
            client: client,
            query_endpoint: query_endpoint,
            write_endpoint: write_endpoint,
        })
    }

    fn create_db(&self) -> Result<(), Box<Error>> {
        let mut u = self.query_endpoint.clone();
        u.query_pairs_mut()
            .append_pair("q", &format!("CREATE DATABASE {}", self.name));
        let mut res = self.client.post(u).send()?;

        check_result(&mut res)?;
        Ok(())
    }

    fn drop_db(&self) -> Result<(), Box<Error>> {
        let mut u = self.query_endpoint.clone();
        u.query_pairs_mut()
            .append_pair("q", &format!("DROP DATABASE {}", self.name));
        let mut res = self.client.post(u).send()?;

        check_result(&mut res)?;
        Ok(())
    }

    fn add_data(&self, data: &str) -> Result<(), Box<Error>> {
        let mut res = self.client.post(self.write_endpoint.clone())
            .body(data)
            .send()?;

        check_result(&mut res)?;
        Ok(())
    }

    fn query<T>(&self, query: &str) -> Result<T, Box<Error>>
        where T: serde::Deserialize,
    {
        let mut u = self.query_endpoint.clone();
        u.query_pairs_mut()
            .append_pair("db", &self.name)
            .append_pair("q", query);
        let mut res = self.client.get(u).send()?;

        check_result(&mut res)?;
        let val = res.json()?;
        Ok(val)
    }
}

impl Drop for TestingDb {
    fn drop(&mut self) {
        // Influx does not care if we drop a database that doesn't exist
        self.drop_db().unwrap_or_else(|e| panic!("Unable to drop database {}: {}", self.name, e));
    }
}

fn check_result(res: &mut reqwest::Response) -> Result<(), Box<Error>> {
    let status = *res.status();

    if status.is_success() {
        Ok(())
    } else if status.is_client_error() {
        Err(parse_influx_server_error(res, "Client error").into())
    } else if status.is_server_error() {
        Err(parse_influx_server_error(res, "Server error").into())
    } else {
        Err(parse_influx_server_error(res, "Unknown error").into())
    }
}

fn parse_influx_server_error(res: &mut reqwest::Response, kind: &str) -> String {
    match res.json::<InfluxServerError>() {
        Ok(e) => format!("{}: {}", kind, e.error),
        Err(_) => format!("{}: (unable to parse error message)", kind),
    }
}
