use std::env;
use std::ops::Range;
use std::cmp::Ordering;
use std::marker::PhantomData;

mod country;
mod location;
mod location_db;

pub use country::Country;
pub use location::{Location, Province, City};
pub use location_db::{ PROVINCES_DB, CITIES_DB };


pub static IP_DB: &'static [u8] = include_bytes!("ip_db.bin");

const V4_RECORD_SIZE: usize = std::mem::size_of::<Record<u32>>();
const V6_RECORD_SIZE: usize = std::mem::size_of::<Record<u128>>();

pub struct Record<T: Sized> {
    pub start: T,
    pub end: T,
    pub location_id: u64,
}

impl Record<u32> {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < V4_RECORD_SIZE {
            return None;
        }
        let start = u32::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3]
        ]);
        let end = u32::from_le_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7]
        ]);
        let location_id = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        Some(Record { start, end, location_id })
    }
}

impl Record<u128> {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < V6_RECORD_SIZE {
            return None;
        }
        let start = u128::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let end = u128::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19],
            bytes[20], bytes[21], bytes[22], bytes[23],
            bytes[24], bytes[25], bytes[26], bytes[27],
            bytes[28], bytes[29], bytes[30], bytes[31],
        ]);
        let location_id = u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35],
            bytes[36], bytes[37], bytes[38], bytes[39],
        ]);
        
        Some(Record { start, end, location_id })
    }
}

struct IpDb {
    v4_records_range: Range<usize>,
    v4_records_len: usize,
    v4_record_size: usize,

    v6_records_range: Range<usize>,
    v6_records_len: usize,
    v6_record_size: usize,
}

impl IpDb {
    pub fn new() -> Self {
        let v4_db_data_zone_start = u32::from_le_bytes([ IP_DB[0], IP_DB[1], IP_DB[2], IP_DB[3] ]) as usize;
        let v4_db_data_zone_end   = u32::from_le_bytes([ IP_DB[4], IP_DB[5], IP_DB[6], IP_DB[7] ]) as usize;
        let v6_db_data_zone_start = u32::from_le_bytes([ IP_DB[8], IP_DB[9], IP_DB[10], IP_DB[11] ]) as usize;
        let v6_db_data_zone_end   = u32::from_le_bytes([ IP_DB[12], IP_DB[13], IP_DB[14], IP_DB[15] ]) as usize;

        let v4_record_size   = std::mem::size_of::<Record<u32>>();
        let v4_records_range = v4_db_data_zone_start .. v4_db_data_zone_end;
        let v4_records_len   = (v4_db_data_zone_end - v4_db_data_zone_start) / v4_record_size;

        let v6_record_size   = std::mem::size_of::<Record<u128>>();
        let v6_records_range = v6_db_data_zone_start .. v6_db_data_zone_end;
        let v6_records_len   = (v6_db_data_zone_end - v6_db_data_zone_start) / v6_record_size;

        Self { v4_record_size, v4_records_range, v4_records_len, v6_record_size, v6_records_range, v6_records_len }
    }

    pub fn query(&self, addr: &std::net::IpAddr) -> Option<Location> {
        match addr {
            std::net::IpAddr::V4(v4_addr) => {
                let v4_number = u32::from(*v4_addr);
                let records = V4Records { range: self.v4_records_range.clone(), len: self.v4_records_len };
                records.binary_search(v4_number)
                    .map(|record| Location(record.location_id) )
            },
            std::net::IpAddr::V6(v6_addr) => {
                let v6_number = u128::from(*v6_addr);
                let records = V6Records { range: self.v6_records_range.clone(), len: self.v6_records_len };
                records.binary_search(v6_number)
                    .map(|record| Location(record.location_id) )
            },
        }
    }
}

struct V6Records {
    range: Range<usize>,
    len: usize,
}

struct V4Records {
    range: Range<usize>,
    len: usize,
}

impl V4Records {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, idx: usize) -> Option<Record<u32>> {
        if idx >= self.len {
            return None;
        }
        
        let offset = idx * V4_RECORD_SIZE;
        if offset + V4_RECORD_SIZE > self.range.end {
            return None;
        }

        let bytes = &IP_DB[offset..offset+V4_RECORD_SIZE];
        
        Record::<u32>::from_bytes(bytes)
    }

    pub unsafe fn get_unchecked(&self, idx: usize) -> Record<u32> {
        unimplemented!()
    }

    pub fn binary_search(&self, x: u32) -> Option<Record<u32>> {
        let s = self;

        let mut size = s.len();
        if size == 0 {
            return None;
        }

        let mut base = 0usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...
            // let cmp = f(unsafe { s.get_unchecked(mid) });
            let item = s.get(mid).unwrap();
            let cmp = {
                if x >= item.start && x <= item.end {
                    Ordering::Equal
                } else if x > item.end {
                    Ordering::Less
                } else if x < item.start {
                    Ordering::Greater
                } else {
                    unreachable!()
                }
            };
            base = if cmp == Ordering::Greater { base } else { mid };
            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        // let cmp = f(unsafe { s.get_unchecked(base) });
        let item = s.get(base).unwrap();
        let cmp = {
            if x >= item.start && x <= item.end {
                return Some(item);
            } else if x > item.end {
                return None;
            } else if x < item.start {
                return None;
            } else {
                unreachable!()
            }
        };
    }
}

impl V6Records {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, idx: usize) -> Option<Record<u128>> {
        if idx >= self.len {
            return None;
        }
        
        let offset = idx * V6_RECORD_SIZE;
        if offset + V6_RECORD_SIZE > self.range.end {
            return None;
        }

        let bytes = &IP_DB[offset..offset+V6_RECORD_SIZE];
        
        Record::<u128>::from_bytes(bytes)
    }

    pub unsafe fn get_unchecked(&self, idx: usize) -> Record<u128> {
        unimplemented!()
    }

    pub fn binary_search(&self, x: u128) -> Option<Record<u128>> {
        let s = self;

        let mut size = s.len();
        if size == 0 {
            return None;
        }

        let mut base = 0usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...
            // let cmp = f(unsafe { s.get_unchecked(mid) });
            let item = s.get(mid).unwrap();
            let cmp = {
                if x >= item.start && x <= item.end {
                    Ordering::Equal
                } else if x > item.end {
                    Ordering::Less
                } else if x < item.start {
                    Ordering::Greater
                } else {
                    unreachable!()
                }
            };
            base = if cmp == Ordering::Greater { base } else { mid };
            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        // let cmp = f(unsafe { s.get_unchecked(base) });
        let item = s.get(base).unwrap();
        let cmp = {
            if x >= item.start && x <= item.end {
                return Some(item);
            } else if x > item.end {
                return None;
            } else if x < item.start {
                return None;
            } else {
                unreachable!()
            }
        };
    }
}

pub fn query(addr: &std::net::IpAddr) -> Option<Location> {
    let db = IpDb::new();
    db.query(addr)
}

fn usage() {
    println!("
    Example:
        $ ip2location 8.8.8.8
    ");
    std::process::exit(0);
}

fn main() {
    let mut args = env::args();
    args.next().unwrap();

    if let Some(params) = args.next() {
        match params.parse::<std::net::IpAddr>() {
            Ok(addr) => {
                let res = query(&addr);
                println!("Query: {}  --> {:?}", addr, res);
            },
            Err(_) => {
                return usage();
            },
        }
    } else {
        return usage();
    }
}