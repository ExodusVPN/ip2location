use std::fmt;
use std::str::FromStr;

use crate::country::Country;
use crate::location_db::{ PROVINCES_DB, CITIES_DB };

// CC:        242  u8 
// PAD:            u8
// Province:  3208 u16
// City:     73496 u32
// Bytes 64
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Location(pub(crate) u64);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Province(pub(crate) u16);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct City(pub(crate) u32);


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

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "{},{} {:?}",
            self.province().map(|province| format!("{:?}", province) ).unwrap_or("Unknow".to_string()),
            self.city().map(|city| format!("{:?}", city) ).unwrap_or("Unknow".to_string()),
            self.country(),
        )
    }
}


impl Province {
    pub fn index(&self) -> u16 {
        self.0
    }

    fn name(&self) -> &'static str {
        PROVINCES_DB[self.0 as usize]
    }
}

impl Into<u16> for Province {
    fn into(self) -> u16 {
        self.0
    }
}

impl FromStr for Province {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PROVINCES_DB
            .binary_search(&s)
            .map(|idx| Province(idx as u16))
            .map_err(|_| ())
    }
}

impl fmt::Debug for Province {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "{:?}", self.name())
    }
}


impl City {
    pub fn index(&self) -> u32 {
        self.0
    }

    fn name(&self) -> &'static str {
        CITIES_DB[self.0 as usize]
    }
}

impl Into<u32> for City {
    fn into(self) -> u32 {
        self.0
    }
}

impl FromStr for City {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CITIES_DB
            .binary_search(&s)
            .map(|idx| City(idx as u32))
            .map_err(|_| ())
    }
}

impl fmt::Debug for City {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "{:?}", self.name())
    }
}
