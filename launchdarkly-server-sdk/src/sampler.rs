use rand::Rng;

pub trait Sampler {
    fn sample(&mut self, ratio: u32) -> bool;
}

pub struct ThreadRngSampler<R: Rng> {
    rng: R,
}

impl<R: Rng> ThreadRngSampler<R> {
    pub fn new(rng: R) -> Self {
        ThreadRngSampler { rng }
    }
}

impl<R: Rng> Sampler for ThreadRngSampler<R> {
    fn sample(&mut self, ratio: u32) -> bool {
        if ratio == 0 {
            return false;
        }

        if ratio == 1 {
            return true;
        }

        self.rng.random_ratio(1, ratio)
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;

    #[test]
    fn test_zero_is_false() {
        let mut sampler = ThreadRngSampler::new(rand::rng());
        assert!(!sampler.sample(0));
    }

    #[test]
    fn test_one_is_true() {
        let mut sampler = ThreadRngSampler::new(rand::rng());
        assert!(sampler.sample(1));
    }

    #[test]
    fn test_can_affect_sampling_ratio() {
        let rng = StdRng::seed_from_u64(0);
        let mut sampler = ThreadRngSampler::new(rng);
        let sampled_size = (0..1_000).filter(|_| sampler.sample(10)).count();

        assert_eq!(sampled_size, 110);
    }
}
