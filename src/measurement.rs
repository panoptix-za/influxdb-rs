use std::fmt::Write;
use std::time::{self, SystemTime};

pub trait Measurement {
    fn to_data(&self, &mut String);
}

impl<'a, T> Measurement for &'a T
    where T: ?Sized + Measurement
{
    fn to_data(&self, bytes: &mut String) {
        (**self).to_data(bytes)
    }
}

impl<T> Measurement for Box<T>
    where T: ?Sized + Measurement
{
    fn to_data(&self, bytes: &mut String) {
        (**self).to_data(bytes)
    }
}

impl<T> Measurement for [T]
    where T: Measurement
{
    fn to_data(&self, bytes: &mut String) {
        for item in self.iter() {
            item.to_data(bytes);
            bytes.push_str("\n");
        }
    }
}

impl<T> Measurement for Vec<T>
    where T: Measurement
{
    fn to_data(&self, bytes: &mut String) {
        self[..].to_data(bytes)
    }
}

impl<'a> Measurement for &'a str {
    fn to_data(&self, bytes: &mut String) {
        bytes.push_str(self);
    }
}

impl Measurement for String {
    fn to_data(&self, bytes: &mut String) {
        self.as_str().to_data(bytes)
    }
}

pub struct Tag<'a> {
    name: &'a str,
    value: &'a str,
}

impl<'a> Tag<'a> {
    /// The name and value are not currently escaped
    pub fn new(name: &'a str, value: &'a str) -> Tag<'a> {
        Tag { name: name, value: value }
    }

    pub fn append(&self, data: &mut String) {
        data.push_str(self.name);
        data.push_str("=");
        data.push_str(self.value);
    }
}

pub trait FieldValue {
    fn append(&self, &mut String);
}

impl<'a, T> FieldValue for &'a T
    where T: ?Sized + FieldValue,
{
    fn append(&self, data: &mut String) {
        (**self).append(data)
    }
}

impl<T> FieldValue for Box<T>
    where T: ?Sized + FieldValue,
{
    fn append(&self, data: &mut String) {
        (**self).append(data)
    }
}

macro_rules! floating_point_field {
    ($($typ: ty),* ) => {
        $(
        impl FieldValue for $typ {
            fn append(&self, data: &mut String) {
                write!(data, "{}", self).expect("Unable to write floating point number")
            }
        }
        )*
    }
}

floating_point_field!(f32, f64);

macro_rules! integer_field {
    ($($typ: ty),* ) => {
        $(
        impl FieldValue for $typ {
            fn append(&self, data: &mut String) {
                write!(data, "{}i", self).expect("Unable to write integral number")
            }
        }
        )*
    }
}

// u64 is **not** supported by InfluxDB
integer_field!(i8, i16, i32, i64, u8, u16, u32);

impl FieldValue for bool {
    fn append(&self, data: &mut String) {
        if *self {
            data.push_str("T");
        } else {
            data.push_str("F");
        }
    }
}

// TODO: escaping of values
impl FieldValue for str {
    fn append(&self, data: &mut String) {
        write!(data, r#""{}""#, self).expect("Unable to write string")
    }
}

impl FieldValue for String {
    fn append(&self, data: &mut String) {
        self.as_str().append(data)
    }
}

pub struct Field<'a, T: 'a> {
    name: &'a str,
    value: &'a T,
}

impl<'a, T> Field<'a, T>
    where T: FieldValue + 'a
{
    /// The name and value are not currently escaped
    pub fn new(name: &'a str, value: &'a T) -> Field<'a, T> {
        Field { name: name, value: value }
    }

    pub fn append(&self, data: &mut String) {
        data.push_str(self.name);
        data.push_str("=");
        self.value.append(data)
    }
}

pub struct Timestamp<'a> {
    value: &'a SystemTime,
}

impl<'a> Timestamp<'a> {
    pub fn new(time: &SystemTime) -> Timestamp {
        Timestamp { value: time }
    }

    pub fn append(&self, data: &mut String) {
        const NANOSECONDS_PER_SECOND: u64 = 1_000_000_000;

        let duration = self.value.duration_since(time::UNIX_EPOCH)
            .expect("Timestamp must come after the UNIX epoch");
        let seconds_as_nanoseconds = duration.as_secs() * NANOSECONDS_PER_SECOND;
        // Truncating from u64 to i64 shouldn't impact us for a long time
        let timestamp = seconds_as_nanoseconds as i64 + duration.subsec_nanos() as i64;
        write!(data, "{}", timestamp).expect("Unable to write timestamp");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn f32_fields_can_be_serialized() {
        assert_eq!(field(3.4f32), "3.4");
    }

    #[test]
    fn f64_fields_can_be_serialized() {
        assert_eq!(field(3.4f64), "3.4");
    }

    #[test]
    fn i8_fields_can_be_serialized() {
        assert_eq!(field(-42i8), "-42i");
    }

    #[test]
    fn i16_fields_can_be_serialized() {
        assert_eq!(field(-42i16), "-42i");
    }

    #[test]
    fn i32_fields_can_be_serialized() {
        assert_eq!(field(-42i32), "-42i");
    }

    #[test]
    fn i64_fields_can_be_serialized() {
        assert_eq!(field(-42i64), "-42i");
    }

    #[test]
    fn u8_fields_can_be_serialized() {
        assert_eq!(field(42u8), "42i");
    }

    #[test]
    fn u16_fields_can_be_serialized() {
        assert_eq!(field(42u16), "42i");
    }

    #[test]
    fn u32_fields_can_be_serialized() {
        assert_eq!(field(42u32), "42i");
    }

    #[test]
    fn boolean_fields_can_be_serialized() {
        assert_eq!(field(true), "T");
        assert_eq!(field(false), "F");
    }

    #[test]
    fn timestamps_can_be_serialized() {
        let s = timestamp(SystemTime::now());
        // We don't control the clock, so the best we can do is make
        // sure the timestamp looks to be in about the right format.
        assert!(s.starts_with("1"));
        assert_eq!(s.len(), 19);
    }

    fn field<T>(val: T) -> String
        where T: FieldValue,
    {
        let mut s = String::new();
        val.append(&mut s);
        s
    }

    fn timestamp(val: SystemTime) -> String {
        let mut s = String::new();
        Timestamp::new(&val).append(&mut s);
        s
    }
}
