use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

pub struct Profiler {
    measurements: Mutex<HashMap<&'static str, Measurement>>
}

#[derive(Debug)]
pub struct Measurement {
    duration: Duration,
    count: u64
}

static PROFILER: OnceLock<Profiler> = OnceLock::new();

impl Profiler {
    fn get() -> &'static Profiler {
        PROFILER.get_or_init(|| Profiler {
            measurements: Mutex::new(HashMap::new())
        })
    }

    pub fn start(name: &'static str) -> ProfilerGuard {
        ProfilerGuard {
            name,
            start: std::time::Instant::now()
        }
    }

    pub fn print_results() {
        let measurements = Profiler::get().measurements.lock().unwrap();

        let mut items = measurements.iter().collect::<Vec<_>>();
        items.sort_by(|a, b| a.1.duration.cmp(&b.1.duration).reverse());

        for (name, duration) in items {
            println!("{}: {:?}", name, duration);
        }
    }

    pub fn reset() {
        let mut measurements = Profiler::get().measurements.lock().unwrap();
        measurements.clear();
    }

}

pub struct ProfilerGuard {
    name: &'static str,
    start: std::time::Instant,
}

impl Drop for ProfilerGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let mut measurements = Profiler::get().measurements.lock().unwrap();
        let entry = measurements.entry(self.name);
        match entry {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let current = entry.get_mut();
                current.duration += elapsed;
                current.count += 1;
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Measurement {
                    duration: elapsed,
                    count: 1
                });
            }
        }
    }
}