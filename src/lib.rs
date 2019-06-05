#[macro_use]
extern crate futures;

#[macro_use]
extern crate log;

#[cfg(test)]
#[macro_use]
extern crate maplit;

pub mod client;
pub mod eval;
mod eventsource;
mod store;
mod update_processor;
pub mod users;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
