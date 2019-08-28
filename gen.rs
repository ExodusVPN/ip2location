use std::io::{self, Write, BufRead, BufReader};
use std::fs::{File, OpenOptions};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::collections::HashSet;
use std::time::SystemTime;


#[path = "src/country.rs"]
mod country;

pub use country::Country;


const V4_DATA: &str = "data/IP2LOCATION-LITE-DB3.CSV";
const V6_DATA: &str = "data/IP2LOCATION-LITE-DB3.IPV6.CSV";


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum IpVersion {
    V4,
    V6,
}

#[derive(Debug)]
pub struct Record {
    pub start: IpAddr,
    pub end: IpAddr,
    pub country: Country,
    pub province: Option<String>,
    pub city: Option<String>,
}

impl Record {
    pub fn is_ipv4(&self) -> bool {
        self.start.is_ipv4()
    }

    pub fn is_ipv6(&self) -> bool {
        self.start.is_ipv6()
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    pub start: &'a str,
    pub end: &'a str,
    pub cc: &'a str,
    pub country: &'a str,
    pub province: &'a str,
    pub city: &'a str,
}

fn eat<'a>(bytes: &'a [u8]) -> Option<Row<'a>> {
    let mut column_start = None;
    let mut column_end = None;
    let mut column_cc = None;
    let mut column_country = None;
    let mut column_province = None;
    let mut column_city = None;

    let bytes_len = bytes.len();
    let mut seq = 0;
    let mut idx = 0;
    while idx < bytes_len {
        let byte = bytes[idx];
        if byte == b'"' {
            idx += 1;
            let offset_start = idx;
            while idx < bytes_len {
                let byte = bytes[idx];
                if byte == b'"' {
                    let offset_end = idx;
                    let v = unsafe {
                        std::str::from_utf8_unchecked(&bytes[offset_start..offset_end])
                    };
                    match seq {
                        0 => column_start = Some(v),
                        1 => column_end = Some(v),
                        2 => column_cc = Some(v),
                        3 => column_country = Some(v),
                        4 => column_province = Some(v),
                        5 => column_city = Some(v),
                        _ => unreachable!(),
                    }
                    seq += 1;
                    break;
                }
                idx += 1;
            }
        }
        idx += 1;
    }

    let start = column_start?;
    let end = column_end?;
    let cc = column_cc?;
    let country = column_country?;
    let province = column_province?;
    let city = column_city?;

    Some(Row { start, end, cc, country, province, city })
}

// CC:        242  u8 
// PAD:            u8
// Province:  3208 u16
// City:     73496 u32
// Bytes 64
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Location(u64);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Province(u16);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct City(u32);

impl Location {
    pub fn new(country_index: u8, province_index: u16, city_index: u32) -> Self {
        let id = (country_index as u64) << 56
                | (province_index as u64) << 32
                | (city_index as u64);
        Self(id)
    }

    pub fn country(&self) -> Country {
        Country::from_index((self.0 >> 56) as u8)
    }

    pub fn province(&self) -> Option<Province> {
        let id = ((self.0 & 0b00000000_00000000_11111111_11111111_00000000_00000000_00000000_00000000) >> 32) as u16;
        if id == std::u16::MAX {
            None
        } else {
            Some(Province(id))
        }
    }

    pub fn city(&self) -> Option<City> {
        let id = (self.0 & 0b00000000_00000000_00000000_00000000_11111111_11111111_11111111_11111111) as u32;
        if id == std::u32::MAX {
            None
        } else {
            Some(City(id))
        }
    }
}

fn parse<'a>(version: IpVersion,
             line: &'a str,
             provinces: &mut HashSet<String>,
             cities: &mut HashSet<String>) -> Option<Record> {
    let row = eat(line.as_bytes())?;

    // NOTE: 如果国家信息是未知的话，那么这条记录没有任何意义。
    let country = row.cc.parse::<Country>().ok()?;
    let province = row.province;
    let city = row.city;

    if province != "-" && !provinces.contains(province) {
        provinces.insert(province.to_string());
    }
    if city != "-" && !cities.contains(city) {
        cities.insert(city.to_string());
    }
    
    let (start, end) = match version {
        IpVersion::V4 => {
            let a = IpAddr::from(Ipv4Addr::from(row.start.parse::<u32>().ok()?));
            let b = IpAddr::from(Ipv4Addr::from(row.end.parse::<u32>().ok()?));
            (a, b)
        },
        IpVersion::V6 => {
            let a = IpAddr::from(Ipv6Addr::from(row.start.parse::<u128>().ok()?));
            let b = IpAddr::from(Ipv6Addr::from(row.end.parse::<u128>().ok()?));
            (a, b)
        }
    };

    let province = if province == "-" { None } else { Some(province.to_string()) };
    let city = if city == "-" { None } else { Some(city.to_string()) };

    Some(Record { start, end, country, province, city })
}

fn main() -> Result<(), io::Error> {
    let now = SystemTime::now();

    let mut v4_records: Vec<Record> = Vec::new();
    let mut v6_records: Vec<Record> = Vec::new();

    let mut provinces: HashSet<String> = HashSet::new();
    let mut cities: HashSet<String> = HashSet::new();

    let v4_data_file = BufReader::new(File::open(V4_DATA)?);
    let v6_data_file = BufReader::new(File::open(V6_DATA)?);
    
    for line in v4_data_file.lines() {
        let line = line?;
        match parse(IpVersion::V4, &line, &mut provinces, &mut cities) {
            Some(record) => v4_records.push(record),
            None => println!("Droped: {}", line),
        }
    }

    for line in v6_data_file.lines() {
        let line = line?;
        match parse(IpVersion::V6, &line, &mut provinces, &mut cities) {
            Some(record) => v6_records.push(record),
            None => println!("Droped: {}", line),
        }
    }

    let mut provinces = provinces.into_iter().collect::<Vec<String>>();
    let mut cities = cities.into_iter().collect::<Vec<String>>();

    provinces.sort();
    cities.sort();

    assert!(provinces.len() < std::u16::MAX as usize);
    assert!(cities.len() < std::u32::MAX as usize);

    // codegen
    let mut file = OpenOptions::new()
                    .create(true)
                    .read(false)
                    .write(true)
                    .append(false)
                    .open("src/location.rs")?;
    
    let code = format!("
// CC:        242  u8 
// PAD:            u8
// Province:  3208 u16
// City:     73496 u32
// Bytes 64
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Location(pub(crate) u64);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Province(pub(crate) u16);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct City(pub(crate) u32);

pub const PROVINCES_LEN: usize = {};
pub static PROVINCES: [&'static str; PROVINCES_LEN] = {:?};

pub const CITIES_LEN: usize = {};
pub static CITIES: [&'static str; CITIES_LEN] = {:?};

", provinces.len(), provinces, cities.len(), cities);
    file.write(code.as_bytes())?;

    let mut v4_file = OpenOptions::new()
                    .create(true)
                    .read(false)
                    .write(true)
                    .append(false)
                    .open("src/v4_db.rs")?;
    let mut v6_file = OpenOptions::new()
                    .create(true)
                    .read(false)
                    .write(true)
                    .append(false)
                    .open("src/v6_db.rs")?;
    
    let v4_records = v4_records.iter().map(|record| {
        let country_id = record.country.index();
        let province_id = match &record.province {
            Some(s) => {
                provinces
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u16)
                    .unwrap_or(std::u16::MAX)
            },
            None => std::u16::MAX,
        };
        let city_id = match &record.city {
            Some(s) => {
                cities
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u32)
                    .unwrap_or(std::u32::MAX)
            },
            None => std::u32::MAX,
        };

        let loc_id: u64 = Location::new(country_id, province_id, city_id).0;
        let start = match record.start {
            IpAddr::V4(addr) => u32::from(addr),
            IpAddr::V6(_) => unreachable!(),
        };
        let end = match record.end {
            IpAddr::V4(addr) => u32::from(addr),
            IpAddr::V6(_) => unreachable!(),
        };

        ( start, end, loc_id )
    }).collect::<Vec<(u32, u32, u64)>>();

    let v6_records = v6_records.iter().map(|record| {
        let country_id = record.country.index();
        let province_id = match &record.province {
            Some(s) => {
                provinces
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u16)
                    .unwrap_or(std::u16::MAX)
            },
            None => std::u16::MAX,
        };
        let city_id = match &record.city {
            Some(s) => {
                cities
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u32)
                    .unwrap_or(std::u32::MAX)
            },
            None => std::u32::MAX,
        };

        let loc_id: u64 = Location::new(country_id, province_id, city_id).0;
        let start = match record.start {
            IpAddr::V4(_) => unreachable!(),
            IpAddr::V6(addr) => u128::from(addr),
        };
        let end = match record.end {
            IpAddr::V4(_) => unreachable!(),
            IpAddr::V6(addr) => u128::from(addr),
        };

        ( start, end, loc_id )
    }).collect::<Vec<(u128, u128, u64)>>();

    let v4_code = format!("
pub const V4_DB_LEN: usize = {};
pub static V4_DB: [(u32, u32, u64); V4_DB_LEN] = {:?};
", v4_records.len(), v4_records);
    let v6_code = format!("
pub const V6_DB_LEN: usize = {};
pub static V6_DB: [(u128, u128, u64); V6_DB_LEN] = {:?};
", v6_records.len(), v6_records);
    
    v4_file.write(v4_code.as_bytes())?;
    v6_file.write(v6_code.as_bytes())?;

    println!("{:?}", now.elapsed());

    Ok(())
}