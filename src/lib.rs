extern crate tokio_core;
extern crate hyper;
extern crate futures;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate quick_error;

use futures::{Future, Stream};
use hyper::client::HttpConnector;
use tokio_core::reactor::Handle;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Url(error: hyper::error::ParseError) {
            from()
        }
        Hyper(error: hyper::Error) {
            from()
        }
        Serde(error: serde_json::Error) {
            from()
        }
    }
}

type Result<T> = ::std::result::Result<T, Error>;

pub struct AsyncDb {
    name: String,
    query_endpoint: hyper::Url,
    client: hyper::Client<HttpConnector>,
}

impl AsyncDb {
    pub fn new(handle: Handle, base_url: &str, name: &str) -> Result<Self> {
        let base_url = hyper::Url::parse(base_url)?;
        let query_endpoint = base_url.join("/query")?;
        let client = hyper::Client::configure().keep_alive(false).build(&handle);

        Ok(AsyncDb {
            name: name.into(),
            query_endpoint: query_endpoint,
            client: client,
        })
    }

    pub fn query(&self, query: &str) -> Query {
        let mut query_endpoint = self.query_endpoint.clone();
        query_endpoint.query_pairs_mut()
            .append_pair("db", &self.name)
            .append_pair("q", query);

        let response = self.client.get(query_endpoint);

        let r2 = response
            .map_err(Error::Hyper)
            // TODO: status code check
            .and_then(|resp| {
                resp.body()
                    .map_err(Error::Hyper)
                    .fold(Vec::new(), |mut acc, chunk| {
                        acc.extend_from_slice(&*chunk);
                        futures::future::ok::<_, Error>(acc) // TODO: best idea?
                    })
            })
            .and_then(|data| serde_json::from_slice(&data).map_err(Error::Serde));

        Query(Box::new(r2))
    }
}

pub struct Query(Box<Future<Item = QueryResponse, Error = Error>>);

impl Future for Query {
    type Item = QueryResponse;
    type Error = Error;

    fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryResponse {
    pub results: Vec<QueryResult>,
}

#[derive(Debug, Deserialize)]
pub struct QueryResult {
    #[serde(default)]
    pub series: Vec<Series>,
    pub statement_id: usize, // TODO: correct integer type?
}

#[derive(Debug, Deserialize)]
pub struct Series {
    pub name: String,
    pub columns: Vec<String>, // TODO: `time` is always added?
    pub values: Vec<Vec<serde_json::Value>>, // TODO: matches with columns?
    // TODO: Don't expose serde types publically
}
