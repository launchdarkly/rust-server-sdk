#[macro_use]
extern crate log;

#[cfg(test)]
#[macro_use]
extern crate maplit;

#[cfg(test)]
extern crate spectral;

pub mod client;
pub mod eval;
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
