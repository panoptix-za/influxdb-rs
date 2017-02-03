extern crate influxdb;
extern crate rand;
extern crate futures;
extern crate tokio_core;

use std::fmt::Write;

use futures::Stream;
use tokio_core::reactor::Core;

/// The address of the InfluxDB server
const BASE_URL: &'static str = "http://localhost:8086/";

/// The InfluxDB database to load the data into
const DB_NAME: &'static str = "load_data";

/// How many pieces of data to submit
const LIMIT: usize = 1_000;

/// How many pieces of data to send at the same time. Each additional
/// one uses a socket, which is a limited resource.
const PARALLELISM: usize = 20;

fn main() {
    let mut core = Core::new().expect("Unable to create reactor core");

    let mut rng = rand::thread_rng();

    let db = influxdb::AsyncDb::new(core.handle(), BASE_URL, DB_NAME)
        .expect("Cannot construct database");

    let (successes, failures) = load_random_data(&db, &mut rng, &mut core);

    println!("{} successes, {} failures", successes.len(), failures.len());
    if !failures.is_empty() {
        let mut another_rng = rand::thread_rng();

        println!("Example failures:");
        for example in rand::sample(&mut another_rng, &failures, 10) {
            println!("{}", example);
        }
    }
}

fn load_random_data<R>(db: &influxdb::AsyncDb, rng: &mut R, core: &mut Core) -> (Vec<()>, Vec<influxdb::Error>)
    where R: rand::Rng,
{
    let mut s = String::new();

    // Generate fake data and add it to the database. Each data
    // addition returns a future.
    let all_futures = rng.gen_iter::<u8>().enumerate().take(LIMIT).map(|(i, val)| {
        s.clear();
        let time = 1434055562000000000 + i;
        write!(&mut s, "cpu_load_short,host=server01,region=us-west value=0.{} {}", val, time)
            .expect("Unable to format string");

        db.add_data(&s)
    });

    // Convert the iterator into a `Stream`. We will process
    // `PARALLELISM` futures at the same time, but with no specified
    // order.
    let all_done =
        futures::stream::iter(all_futures.map(Ok))
        .buffer_unordered(PARALLELISM);

    let mut successes = Vec::with_capacity(LIMIT);
    let mut failures = Vec::with_capacity(LIMIT);

    // Pull values off the stream, dividing them into success and
    // failure buckets.
    let mut all_done = all_done.into_future();
    loop {
        match core.run(all_done) {
            Ok((None, _)) => break,
            Ok((Some(v), next_all_done)) => {
                successes.push(v);
                if successes.len() % 25 == 0 {
                    println!("Successes: {}", successes.len());
                }
                all_done = next_all_done.into_future();
            }
            Err((v, next_all_done)) => {
                failures.push(v);
                if failures.len() % 25 == 0 {
                    println!("Failures: {}", failures.len());
                }
                all_done = next_all_done.into_future();
            }
        }
    }

    (successes, failures)
}
