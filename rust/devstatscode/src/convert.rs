//! Numeric conversion helpers — port of `convert.go`.
//!
//! Go's `GetFloatFromInterface` switches over `interface{}` dynamic types. In
//! Rust the closest equivalents are a trait for statically typed values and a
//! helper for dynamically typed JSON values (the only `interface{}` source
//! DevStats actually feeds it).

/// Something convertible to `f64` if numeric (`Some`), `None` otherwise.
pub trait ToFloat {
    fn to_float(&self) -> Option<f64>;
}

macro_rules! numeric_to_float {
    ($($t:ty),*) => {
        $(impl ToFloat for $t {
            fn to_float(&self) -> Option<f64> {
                Some(*self as f64)
            }
        })*
    };
}

numeric_to_float!(f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize);

impl ToFloat for str {
    fn to_float(&self) -> Option<f64> {
        None
    }
}

impl ToFloat for String {
    fn to_float(&self) -> Option<f64> {
        None
    }
}

impl ToFloat for bool {
    fn to_float(&self) -> Option<f64> {
        None
    }
}

impl ToFloat for serde_json::Value {
    fn to_float(&self) -> Option<f64> {
        self.as_f64()
    }
}

/// Go `GetFloatFromInterface`: `(value, true)` for numeric types, `(NaN, false)`
/// for anything else.
pub fn get_float_from_interface<T: ToFloat + ?Sized>(v: &T) -> (f64, bool) {
    match v.to_float() {
        Some(f) => (f, true),
        None => (f64::NAN, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check<T: ToFloat + ?Sized>(v: &T, expected: f64, ok: bool) {
        let (got, got_ok) = get_float_from_interface(v);
        assert_eq!(got_ok, ok);
        if got.is_nan() && expected.is_nan() {
            return;
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn go_table() {
        check(&0.0f64, 0.0, true);
        check(&1.0f64, 1.0, true);
        check(&-1.5f64, -1.5, true);
        check(&2.0f32, 2.0, true);
        check(&3i64, 3.0, true);
        check(&-33i64, -33.0, true);
        check(&4i32, 4.0, true);
        check(&5i16, 5.0, true);
        check(&6i8, 6.0, true);
        check(&7isize, 7.0, true);
        check(&8u64, 8.0, true);
        check(&9u32, 9.0, true);
        check(&10u16, 10.0, true);
        check(&11u8, 11.0, true);
        check(&12usize, 12.0, true);
        check("123", f64::NAN, false);
        check(&"xyz".to_string(), f64::NAN, false);
        check(&true, f64::NAN, false);
    }

    #[test]
    fn json_values() {
        check(&serde_json::json!(1.5), 1.5, true);
        check(&serde_json::json!(7), 7.0, true);
        check(&serde_json::json!("123"), f64::NAN, false);
        check(&serde_json::json!(null), f64::NAN, false);
    }
}
