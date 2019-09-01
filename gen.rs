use std::time::SystemTime;
use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::fs::{File, OpenOptions};


#[path = "src/country.rs"]
mod country;
#[path = "src/location.rs"]
mod location;

pub use country::Country;
pub use location::{Location, Province, City};


const V4_DATA: &str = "data/IP2LOCATION-LITE-DB3.CSV";
const V6_DATA: &str = "data/IP2LOCATION-LITE-DB3.IPV6.CSV";


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum IpVersion {
    V4,
    V6,
}

#[derive(Debug)]
pub struct Record<'a, IP: std::str::FromStr> {
    pub start: IP,
    pub end: IP,
    pub country: Country,
    pub province: Option<&'a str>,
    pub city: Option<&'a str>,
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

fn parse<'a, IP: std::str::FromStr>(line: &'a str,
                                    provinces: &mut HashSet<&'a str>,
                                    cities: &mut HashSet<&'a str> ) -> Option<Record<'a, IP>> {
    let row = eat(line.as_bytes())?;

    // NOTE: 如果国家信息是未知的话，那么这条记录没有任何意义。
    let country = row.cc.parse::<Country>().ok()?;
    let province = row.province;
    let city = row.city;

    if province != "-" && !provinces.contains(province) {
        provinces.insert(province);
    }
    if city != "-" && !cities.contains(city) {
        cities.insert(city);
    }
    
    let start = row.start.parse::<IP>().ok()?;
    let end = row.end.parse::<IP>().ok()?;

    let province = if province == "-" { None } else { Some(province) };
    let city = if city == "-" { None } else { Some(city) };

    Some(Record { start, end, country, province, city })
}

fn main() -> Result<(), io::Error> {
    let now = SystemTime::now();

    let mut v4_data_file = String::new();
    let mut v6_data_file = String::new();
    File::open(V4_DATA)?.read_to_string(&mut v4_data_file)?;
    File::open(V6_DATA)?.read_to_string(&mut v6_data_file)?;

    let mut v4_records: Vec<Record<'_, u32>> = Vec::new();
    let mut v6_records: Vec<Record<'_, u128>> = Vec::new();

    let mut provinces: HashSet<&str> = HashSet::new();
    let mut cities: HashSet<&str> = HashSet::new();

    for line in v4_data_file.lines() {
        match parse::<u32>(&line, &mut provinces, &mut cities) {
            Some(record) => v4_records.push(record),
            None => {
                println!("Droped: {}", line);
            },
        }
    }

    for line in v6_data_file.lines() {
        match parse::<u128>(&line, &mut provinces, &mut cities) {
            Some(record) => v6_records.push(record),
            None => {
                println!("Droped: {}", line);
            },
        }
    }

    let mut provinces = provinces.into_iter().collect::<Vec<&str>>();
    let mut cities = cities.into_iter().collect::<Vec<&str>>();

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
                    .open("src/location_db.rs")?;
    
    let code = format!("
pub const PROVINCES_DB_LEN: usize = {};
pub static PROVINCES_DB: [&'static str; PROVINCES_DB_LEN] = {:?};

pub const CITIES_DB_LEN: usize = {};
pub static CITIES_DB: [&'static str; CITIES_DB_LEN] = {:?};

", provinces.len(), provinces, cities.len(), cities);
    file.write(code.as_bytes())?;

    // 二进制数据库文件
    let mut ip_db_file = OpenOptions::new()
                    .create(true)
                    .read(false)
                    .write(true)
                    .append(false)
                    .open("src/ip_db.bin")?;
    // Header
    // u32 u32 u32 u32
    let header_len: usize = 4 + 4 + 4 + 4;

    let v4_recod_bin_size: usize = 4 + 4 + 8;
    let v4_recod_bin_len: usize = v4_recod_bin_size * v4_records.len();
    let v6_recod_bin_size: usize = 16 + 16 + 8;
    let v6_recod_bin_len: usize = v6_recod_bin_size * v6_records.len();

    let v4_db_data_zone_start: u32 = header_len as u32;
    let v4_db_data_zone_end: u32 = v4_db_data_zone_start + v4_recod_bin_len as u32;
    let v6_db_data_zone_start: u32 = v4_db_data_zone_end;
    let v6_db_data_zone_end: u32 = v6_db_data_zone_start + v6_recod_bin_len as u32;

    ip_db_file.write_all(&v4_db_data_zone_start.to_le_bytes())?;
    ip_db_file.write_all(&v4_db_data_zone_end.to_le_bytes())?;
    ip_db_file.write_all(&v6_db_data_zone_start.to_le_bytes())?;
    ip_db_file.write_all(&v6_db_data_zone_end.to_le_bytes())?;

    // V4_DATA_ZONE
    for record in v4_records.iter() {
        let country_id = record.country.index();
        let province_id = match record.province {
            Some(s) => {
                provinces
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u16)
                    .unwrap_or(std::u16::MAX)
            },
            None => std::u16::MAX,
        };
        let city_id = match record.city {
            Some(s) => {
                cities
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u32)
                    .unwrap_or(std::u32::MAX)
            },
            None => std::u32::MAX,
        };
        
        let start: u32 = record.start;
        let end: u32 = record.end;
        let loc_id: u64 = Location::new(country_id, province_id, city_id).0;

        ip_db_file.write_all(&start.to_le_bytes())?;
        ip_db_file.write_all(&end.to_le_bytes())?;
        ip_db_file.write_all(&loc_id.to_le_bytes())?;
    }

    // V6_DATA_ZONE
    for record in v6_records.iter() {
        let country_id = record.country.index();
        let province_id = match record.province {
            Some(s) => {
                provinces
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u16)
                    .unwrap_or(std::u16::MAX)
            },
            None => std::u16::MAX,
        };
        let city_id = match record.city {
            Some(s) => {
                cities
                    .binary_search(&s)
                    .ok()
                    .map(|idx| idx as u32)
                    .unwrap_or(std::u32::MAX)
            },
            None => std::u32::MAX,
        };
        
        let start: u128 = record.start;
        let end: u128 = record.end;
        let loc_id: u64 = Location::new(country_id, province_id, city_id).0;

        ip_db_file.write_all(&start.to_le_bytes())?;
        ip_db_file.write_all(&end.to_le_bytes())?;
        ip_db_file.write_all(&loc_id.to_le_bytes())?;
    }

    println!("{:?}", now.elapsed());

    Ok(())
}