extern crate tokio_core;
extern crate futures;
extern crate reqwest;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

extern crate influxdb;

use std::error::Error;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

use futures::Future;

use influxdb::{AsyncDb, QueryResponse};

const HOSTNAME: &'static str = "http://localhost:8086/";

#[test]
fn query_asynchronously() {
    let db = fresh_db();

    db.add_data("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000")
        .unwrap();

    let response = with_core(|core| {
        let async_db = AsyncDb::new(core.handle(), HOSTNAME, &db.name).unwrap();

        async_db.query(r#"SELECT "value","host" FROM "cpu_load_short" WHERE "region"='us-west'"#)
    });

    assert_eq!(response.results[0].series[0].name, "cpu_load_short");
    assert_eq!(response.results[0].series[0].values[0][1].as_f64(), Some(0.64));
    assert_eq!(response.results[0].series[0].values[0][2].as_str(), Some("server01"));
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
    let db = TestingDb::new(HOSTNAME, name)
        .expect("Unable to create test database");
    db.create_db()
        .expect("Unable to create test database");
    db
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
        // TODO: What happens if we drop twice?
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

#[derive(Debug, Deserialize)]
struct InfluxServerError {
    error: String,
}

fn parse_influx_server_error(res: &mut reqwest::Response, kind: &str) -> String {
    match res.json::<InfluxServerError>() {
        Ok(e) => format!("{}: {}", kind, e.error),
        Err(_) => format!("{}: (unable to parse error message)", kind),
    }
}
