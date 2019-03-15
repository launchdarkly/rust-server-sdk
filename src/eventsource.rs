use std::collections::BTreeMap as Map;
use std::str::from_utf8;

use futures::future::{self, Future};
use futures::stream::Stream;
use reqwest as r;
use reqwest::r#async as ra;

/*
 * TODO remove debug output
 * TODO reconnect
 * TODO improve error handling (less unwrap)
 * TODO consider lines split across chunks?
 */

pub type Error = String; // TODO enum

#[derive(Debug)]
// TODO can we make this require less copying?
pub struct Event {
    pub event_type: String,
    fields: Map<String, Vec<u8>>,
}

impl Event {
    fn new() -> Event {
        Event {
            event_type: "".to_string(),
            fields: Map::new(),
        }
    }

    pub fn field(&self, name: &str) -> &[u8] {
        &self.fields[name.into()]
    }

    fn set_field(&mut self, name: &str, value: &[u8]) {
        self.fields.insert(name.into(), value.to_owned());
    }
}

impl std::ops::Index<&str> for Event {
    type Output = [u8];

    fn index(&self, name: &str) -> &[u8] {
        &self.fields[name.into()]
    }
}

pub type EventStream = Box<Stream<Item = Event, Error = Error> + Send>;

pub struct ClientBuilder {
    //http: ra::Client,
    request_builder: ra::RequestBuilder,
}

impl ClientBuilder {
    pub fn header(mut self, key: &str, value: &str) -> ClientBuilder {
        self.request_builder = self.request_builder.header(key, value);
        self
    }

    pub fn build(self) -> Client {
        Client {
            request_builder: self.request_builder,
        }
    }
}

pub struct Client {
    request_builder: ra::RequestBuilder,
}

impl Client {
    pub fn for_url<U: r::IntoUrl>(url: U) -> ClientBuilder {
        let http = ra::Client::new();
        let builder = http.get(url);
        ClientBuilder {
            //http,
            request_builder: builder,
        }
    }

    pub fn stream(self) -> EventStream {
        let resp = self.request_builder.send();

        let fut_stream_chunks = resp
            .and_then(|resp| {
                println!("resp: {:?}", resp);

                future::ok(resp.into_body())
            })
            .map_err(|e| {
                println!("error = {:?}", e);
                e
            });

        Box::new(
            fut_stream_chunks
                .flatten_stream()
                .map_err(|e| format!("error = {:?}", e).to_string())
                .map(|c| decode_chunk(c).expect("bad decode"))
                .filter_map(|opt| opt),
        )
    }
}

fn decode_chunk(chunk: ra::Chunk) -> Result<Option<Event>, Error> {
    println!("decoder got a chunk: {:?}", chunk);

    let mut event: Option<Event> = None;

    let lines = chunk.split(|b| &b'\n' == b);

    for line in lines {
        println!("splat: {:?}", from_utf8(line).unwrap());

        if line.is_empty() {
            println!("emptyline");
            return Ok(event);
        }

        match line[0] {
            b':' => {
                println!(
                    "comment: {}",
                    from_utf8(&line[1..]).unwrap_or("<bad utf-8>")
                );
                continue;
            }
            _ => match line.iter().position(|&b| b':' == b) {
                Some(colon_pos) => {
                    let key = &line[0..colon_pos];
                    let key = from_utf8(key).unwrap();
                    let value = &line[colon_pos + 1..];
                    let value = match value.iter().position(|&b| !b.is_ascii_whitespace()) {
                        Some(start) => &value[start..],
                        None => b"",
                    };

                    if event.is_none() {
                        event = Some(Event::new());
                    }
                    match key {
                        "event" => {
                            event.as_mut().unwrap().event_type =
                                from_utf8(value).unwrap().to_string()
                        }
                        _ => event.as_mut().unwrap().set_field(key, value),
                    };

                    println!("key: {}, value: {}", key, from_utf8(value).unwrap());
                }
                None => {
                    println!("some kind of weird line");
                }
            },
        }
    }

    Err("oops".to_string())
}
