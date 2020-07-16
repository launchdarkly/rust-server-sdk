#[macro_use]
extern crate log;

#[cfg(test)]
#[macro_use]
extern crate maplit;

#[cfg(test)]
extern crate spectral;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

pub mod client;
pub mod eval;
mod event_processor;
mod event_sink;
pub mod events;
pub mod store; // TODO no, move flagvalue into types or something instead
mod update_processor;
pub mod users;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
