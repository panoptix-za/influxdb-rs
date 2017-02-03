extern crate tokio_core;
extern crate hyper;
extern crate futures;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate quick_error;

use std::net::SocketAddr;

use futures::future::Either;
use futures::{Future, Stream, BoxFuture};
use hyper::client::{self, HttpConnector};
use tokio_core::net::UdpSocket;
use tokio_core::reactor::Handle;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Url(error: hyper::error::ParseError) {
            description(error.description())
            display("Unable to parse URL: {}", error)
            from()
            cause(error)
        }
        Hyper(error: hyper::Error) {
            description(error.description())
            display("Unable to perform HTTP request: {}", error)
            from()
            cause(error)
        }
        Serde(error: serde_json::Error) {
            description(error.description())
            display("Unable to deserialize JSON: {}", error)
            from()
            cause(error)
        }
        BadRequest(what: String) {
            description("The InfluxDB server responded with an error")
            display("The InfluxDB server responded with an error: {}", what)
        }
        AddrParse(error: std::net::AddrParseError) {
            description(error.description())
            display("Unable to parse the address: {}", error)
            from()
            cause(error)
        }
        Udp(error: std::io::Error) {
            description(error.description())
            display("Unable to perform UDP request: {}", error)
            from()
            cause(error)
        }
    }
}

type Result<T> = ::std::result::Result<T, Error>;

pub struct AsyncDb {
    name: String,
    query_endpoint: hyper::Url,
    write_endpoint: hyper::Url,
    client: hyper::Client<HttpConnector>,
}

impl AsyncDb {
    pub fn new(handle: Handle, base_url: &str, name: &str) -> Result<Self> {
        let base_url = hyper::Url::parse(base_url)?;
        let query_endpoint = base_url.join("/query")?;
        let mut write_endpoint = base_url.join("/write")?;
        write_endpoint.query_pairs_mut()
            .append_pair("db", &name);

        let client = hyper::Client::configure().keep_alive(false).build(&handle);

        Ok(AsyncDb {
            name: name.into(),
            query_endpoint: query_endpoint,
            write_endpoint: write_endpoint,
            client: client,
        })
    }

    pub fn add_data(&self, data: &str) -> AddData {
        let mut request = client::Request::new(hyper::Method::Post, self.write_endpoint.clone());
        request.set_body(data.to_owned());

        let response =
            self.client.request(request)
            .map_err(Error::Hyper)
            .and_then(check_response_code)
            .map(|_| ());

        AddData(Box::new(response))
    }

    pub fn query(&self, query: &str) -> Query {
        let mut query_endpoint = self.query_endpoint.clone();
        query_endpoint.query_pairs_mut()
            .append_pair("db", &self.name)
            .append_pair("q", query);

        let response =
            self.client.get(query_endpoint)
            .map_err(Error::Hyper)
            .and_then(check_response_code)
            .and_then(response_to_json);

        Query(Box::new(response))
    }
}

fn check_response_code(resp: client::Response) -> Box<Future<Item = client::Response, Error = Error>> {
    let f = if resp.status().is_success() {
        Either::A(futures::future::ok(resp))
    } else {
        let e = response_to_json::<InfluxServerError>(resp)
            .and_then(|message| Err(Error::BadRequest(message.error)));
        Either::B(e)
    };

    Box::new(f)
}

fn response_to_json<T>(resp: client::Response) -> Box<Future<Item = T, Error = Error>>
    where T: serde::Deserialize + 'static,
{
    let f =
        resp.body()
        .map_err(Error::Hyper)
        .fold(Vec::new(), |mut acc, chunk| {
            // TODO: Is it the best idea to just build up a big `Vec`?
            // TODO: Is there some way of reusing the vector allocation?
            acc.extend_from_slice(&*chunk);
            futures::future::ok::<_, Error>(acc)
        })
        .and_then(|body| serde_json::from_slice(&body).map_err(Error::Serde));

    Box::new(f)
}

#[must_use = "futures do nothing unless polled"]
pub struct AddData(Box<Future<Item = (), Error = Error>>);

impl Future for AddData {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

#[must_use = "futures do nothing unless polled"]
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
    pub error: Option<String>,
    pub statement_id: usize,
}

#[derive(Debug, Deserialize)]
pub struct Series {
    pub name: String,
    pub columns: Vec<String>, // TODO: `time` is always added?
    pub values: Vec<Vec<serde_json::Value>>, // TODO: matches with columns?
    // TODO: Don't expose serde types publically
}

#[derive(Debug, Deserialize)]
pub struct InfluxServerError {
    pub error: String,
}

pub struct AsyncUdpDb {
    handle: Handle,
    my_addr: SocketAddr,
    their_addr: SocketAddr,
}

impl AsyncUdpDb {
    pub fn new(handle: Handle, ip_port: &str) -> Result<Self> {
        Ok(AsyncUdpDb {
            handle: handle,
            my_addr: "0.0.0.0:0".parse()?,
            their_addr: ip_port.parse()?,
        })
    }

    pub fn add_data(&self, data: &str) -> AddDataUdp {
        // TODO: We could consume self like `send_dgram` does, which
        // allows reusing the same socket over and over. The API would
        // be more annoying, but might be like other futures...
        let f = match UdpSocket::bind(&self.my_addr, &self.handle) {
            Ok(socket) => {
                let f = socket.send_dgram(data.to_owned(), self.their_addr)
                    .map(|_| ())
                    .map_err(Error::Udp);
                Either::A(f)
            }
            Err(e) => {
                Either::B(futures::future::err(Error::Udp(e)))
            }
        };

        AddDataUdp(f.boxed())
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct AddDataUdp(BoxFuture<(), Error>);

impl Future for AddDataUdp {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}
