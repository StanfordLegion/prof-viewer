use std::collections::BTreeMap;

use crate::data::{TileID, TileSet};
use crate::timestamp::Interval;

pub struct TileManager {
    tile_set: TileSet,
    interval: Interval,
    last_request_interval: (Option<Interval>, Option<Interval>), // full: false, true
    tile_cache: (Vec<TileID>, Vec<TileID>),                      // full: false, true
}

fn select<T>(cond: bool, true_value: T, false_value: T) -> T {
    if cond { true_value } else { false_value }
}

fn fill_cache<T, I, K>(cache: &mut Vec<T>, values: I, last_key: &mut Option<K>, key: K) -> Vec<T>
where
    T: Clone,
    I: IntoIterator<Item = T>,
{
    *last_key = Some(key);
    cache.clear();
    cache.extend(values);
    cache.clone()
}

impl TileManager {
    pub fn new(tile_set: TileSet, interval: Interval) -> Self {
        Self {
            tile_set,
            interval,
            last_request_interval: (None, None),
            tile_cache: (Vec::new(), Vec::new()),
        }
    }

    pub fn request_tiles(&mut self, view_interval: Interval, full: bool) -> Vec<TileID> {
        let last_request_interval = select(
            full,
            &mut self.last_request_interval.1,
            &mut self.last_request_interval.0,
        );
        let tile_cache = select(full, &mut self.tile_cache.1, &mut self.tile_cache.0);

        let request_interval = view_interval.intersection(self.interval);
        if *last_request_interval == Some(request_interval) {
            return tile_cache.clone();
        }

        let request_duration = request_interval.duration_ns();
        if request_duration <= 0 {
            return fill_cache(tile_cache, [], last_request_interval, request_interval);
        }

        let ratio = |level: &Vec<TileID>| {
            let d = level.first().unwrap().0.duration_ns();
            if d < request_duration {
                request_duration as f64 / d as f64
            } else {
                d as f64 / request_duration as f64
            }
        };

        // Dynamic profile.
        if self.tile_set.tiles.is_empty() {
            if let Some(cache_interval) = tile_cache
                .iter()
                .copied()
                .reduce(|a, b| TileID(a.0.union(b.0)))
            {
                // We can use the existing cache if:
                //
                //  1. There is at least partial overlap with the new request.
                //  2. We haven't drifted too far from the tile size requested before.

                if ratio(tile_cache) <= 2.0 {
                    if cache_interval.0.contains_interval(request_interval) {
                        // Interval completely contained in the existing cache, just return it.
                        *last_request_interval = Some(request_interval);
                        return tile_cache.clone();
                    } else if cache_interval.0.overlaps(request_interval) {
                        // Partial overlap, extend the cache in the direction we need.
                        todo!();
                    }
                }
            }

            // Otherwise just return the request as one tile.
            return fill_cache(
                tile_cache,
                [TileID(request_interval)],
                last_request_interval,
                request_interval,
            );
        }

        // We're in a static profile. Choose an appropriate level to load.
        let chosen_level = if full {
            // Full request must always fetch highest level of detail.
            self.tile_set.tiles.last().unwrap()
        } else {
            // Otherwise estimate the best zoom level, where "best" minimizes the
            // ratio of the tile size to request size.
            self.tile_set
                .tiles
                .iter()
                .min_by(|level1, level2| ratio(level1).partial_cmp(&ratio(level2)).unwrap())
                .unwrap()
        };

        // Now filter to just tiles overlapping the requested interval.
        fill_cache(
            tile_cache,
            chosen_level
                .iter()
                .filter(|tile| request_interval.overlaps(tile.0))
                .copied(),
            last_request_interval,
            request_interval,
        )
    }

    pub fn invalidate_cache<T>(tile_ids: &[TileID], cache: &mut BTreeMap<TileID, T>) {
        cache.retain(|tile_id, _| tile_ids.contains(tile_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::timestamp::Timestamp;

    #[test]
    fn request_dynamic_empty() {
        let int = Interval::new(Timestamp(0), Timestamp(10));
        let req = Interval::new(Timestamp(5), Timestamp(5));
        let mut tm = TileManager::new(TileSet::default(), int);
        assert!(tm.request_tiles(req, false).is_empty());
        assert!(tm.request_tiles(req, true).is_empty());
    }

    #[test]
    fn request_static_empty() {
        let int = Interval::new(Timestamp(0), Timestamp(100));
        let req = Interval::new(Timestamp(25), Timestamp(25));
        let ts = TileSet {
            tiles: vec![
                vec![TileID(int)],
                vec![
                    TileID(Interval::new(Timestamp(0), Timestamp(50))),
                    TileID(Interval::new(Timestamp(50), Timestamp(100))),
                ],
            ],
        };
        let mut tm = TileManager::new(ts, int);
        assert!(tm.request_tiles(req, false).is_empty());
        assert!(tm.request_tiles(req, true).is_empty());
    }

    #[test]
    fn request_dynamic_repeat() {
        let int = Interval::new(Timestamp(0), Timestamp(10));
        let req = Interval::new(Timestamp(0), Timestamp(10));
        let mut tm = TileManager::new(TileSet::default(), int);
        // Answer should be stable on repeat queries.
        assert_eq!(tm.request_tiles(req, false), vec![TileID(req)]);
        assert_eq!(tm.request_tiles(req, false), vec![TileID(req)]);
        assert_eq!(tm.request_tiles(req, true), vec![TileID(req)]);
        assert_eq!(tm.request_tiles(req, true), vec![TileID(req)]);
    }

    #[test]
    fn request_static_repeat() {
        let int = Interval::new(Timestamp(0), Timestamp(100));
        let req = Interval::new(Timestamp(10), Timestamp(90));
        let ts = TileSet {
            tiles: vec![
                vec![TileID(int)],
                vec![
                    TileID(Interval::new(Timestamp(0), Timestamp(50))),
                    TileID(Interval::new(Timestamp(50), Timestamp(100))),
                ],
            ],
        };
        let mut tm = TileManager::new(ts, int);
        let part = vec![TileID(Interval::new(Timestamp(0), Timestamp(100)))];
        let full = vec![
            TileID(Interval::new(Timestamp(0), Timestamp(50))),
            TileID(Interval::new(Timestamp(50), Timestamp(100))),
        ];
        // Answer should be stable on repeat queries.
        assert_eq!(&tm.request_tiles(req, false), &part);
        assert_eq!(&tm.request_tiles(req, false), &part);
        assert_eq!(&tm.request_tiles(req, true), &full);
        assert_eq!(&tm.request_tiles(req, true), &full);
        assert_eq!(&tm.request_tiles(req, false), &part);
        assert_eq!(&tm.request_tiles(req, false), &part);
        assert_eq!(&tm.request_tiles(req, true), &full);
        assert_eq!(&tm.request_tiles(req, true), &full);
    }

    #[test]
    fn request_dynamic_zoom() {
        let int = Interval::new(Timestamp(0), Timestamp(100));
        let req90 = Interval::new(Timestamp(0), Timestamp(90));
        let req80 = Interval::new(Timestamp(0), Timestamp(80));
        let req70 = Interval::new(Timestamp(0), Timestamp(70));
        let req60 = Interval::new(Timestamp(0), Timestamp(60));
        let req50 = Interval::new(Timestamp(0), Timestamp(50));
        let req40 = Interval::new(Timestamp(0), Timestamp(40));
        let req30 = Interval::new(Timestamp(0), Timestamp(30));
        let req20 = Interval::new(Timestamp(0), Timestamp(20));
        let req10 = Interval::new(Timestamp(0), Timestamp(10));
        let mut tm = TileManager::new(TileSet::default(), int);
        // Zoom level sticks until we reach the threshold.
        assert_eq!(tm.request_tiles(req90, false), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req80, false), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req70, false), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req60, false), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req50, false), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req40, false), vec![TileID(req40)]);
        assert_eq!(tm.request_tiles(req30, false), vec![TileID(req40)]);
        assert_eq!(tm.request_tiles(req20, false), vec![TileID(req40)]);
        assert_eq!(tm.request_tiles(req10, false), vec![TileID(req10)]);
        assert_eq!(tm.request_tiles(req90, true), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req80, true), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req70, true), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req60, true), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req50, true), vec![TileID(req90)]);
        assert_eq!(tm.request_tiles(req40, true), vec![TileID(req40)]);
        assert_eq!(tm.request_tiles(req30, true), vec![TileID(req40)]);
        assert_eq!(tm.request_tiles(req20, true), vec![TileID(req40)]);
        assert_eq!(tm.request_tiles(req10, true), vec![TileID(req10)]);
    }
}
