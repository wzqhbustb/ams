//! PostgreSQL text/binary value encoding and type-OID mapping.
//!
//! The extended protocol supports Int4, Int8, Float8, Text, JSONB and pg_rust
//! VECTOR parameters and results. Integer and f64 binary payloads use network
//! byte order. JSONB uses OID 114 and text format only; its binary form is not
//! implemented.
//! VECTOR text is `[v0,v1,…]`; its binary payload is `dim:u16`,
//! `unused:u16 = 0`, then `dim` big-endian f32 values. The catalog-owned
//! VECTOR OID is reported instead of disguising vectors as TEXT.
//!
//! Timestamptz and Uuid retain the earlier Simple Query compromise: their
//! text payloads are reported as INT8 and TEXT respectively so stock clients
//! can round-trip them. They are not Extended Query parameter types.

use pg_catalog::builtin_types::VECTOR_OID;
use pg_engine::{ColumnType, Datum, Value};

use crate::error::{Result, WireError};

/// Wire-level type descriptor: the OID and fixed width a RowDescription
/// reports for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireType {
    /// PostgreSQL type OID (see the table in the module docs).
    pub oid: u32,
    /// Fixed width in bytes, or -1 for varlena.
    pub len: i16,
}

/// Map a heap codec type to its wire descriptor (see module docs for the
/// full mapping table).
pub fn wire_type(ty: ColumnType) -> WireType {
    match ty {
        ColumnType::Int4 => WireType { oid: 23, len: 4 },
        ColumnType::Int8 => WireType { oid: 20, len: 8 },
        ColumnType::Float8 => WireType { oid: 701, len: 8 },
        ColumnType::Text => WireType { oid: 25, len: -1 },
        ColumnType::Bytea => WireType { oid: 17, len: -1 },
        ColumnType::Jsonb => WireType { oid: 114, len: -1 },
        // Reported as INT8 / TEXT so the declared OID matches the bytes
        // (module docs). `typlen` 8 for the µs integer.
        ColumnType::Timestamptz => WireType { oid: 20, len: 8 },
        ColumnType::Uuid => WireType { oid: 25, len: -1 },
        ColumnType::Vector(_) => WireType {
            oid: u32::try_from(VECTOR_OID.raw().0).expect("VECTOR OID fits in u32"),
            len: -1,
        },
    }
}

/// Infer a column type from a value when the table schema is unavailable
/// (e.g. the table was dropped between parse and execution). Used only as
/// a fallback; schema-driven typing via [`wire_type`] is the primary path.
pub fn column_type_of(d: &Datum) -> ColumnType {
    match d {
        Datum::Int4(_) => ColumnType::Int4,
        Datum::Int8(_) => ColumnType::Int8,
        Datum::Float8(_) => ColumnType::Float8,
        Datum::Text(_) => ColumnType::Text,
        Datum::Bytea(_) => ColumnType::Bytea,
        Datum::Jsonb(_) => ColumnType::Jsonb,
        Datum::Timestamptz(_) => ColumnType::Timestamptz,
        Datum::Uuid(_) => ColumnType::Uuid,
        Datum::Vector(v) => ColumnType::Vector(v.len() as u32),
        // The underlying type of a TOASTed value is unknowable here; TEXT
        // is the least-wrong placeholder since the encode path errors out
        // on `External` anyway.
        Datum::External(_) => ColumnType::Text,
    }
}

/// PostgreSQL text format code.
pub const FORMAT_TEXT: i16 = 0;
/// PostgreSQL binary format code.
pub const FORMAT_BINARY: i16 = 1;

fn value_error(sqlstate: &'static str, message: impl Into<String>) -> WireError {
    WireError::Value {
        sqlstate,
        message: message.into(),
    }
}

fn invalid_text(message: impl Into<String>) -> WireError {
    value_error("22P02", message)
}

fn invalid_binary(message: impl Into<String>) -> WireError {
    value_error("22P03", message)
}

fn validate_format(format: i16) -> Result<()> {
    if matches!(format, FORMAT_TEXT | FORMAT_BINARY) {
        Ok(())
    } else {
        Err(value_error(
            "22023",
            format!("unsupported format code {format} (expected 0 or 1)"),
        ))
    }
}

/// Expand PostgreSQL's 0/1/N format-code rule over `count` values.
pub fn expand_format_codes(codes: &[i16], count: usize, what: &str) -> Result<Vec<i16>> {
    for code in codes {
        validate_format(*code)?;
    }
    match codes.len() {
        0 => Ok(vec![FORMAT_TEXT; count]),
        1 => Ok(vec![codes[0]; count]),
        n if n == count => Ok(codes.to_vec()),
        n => Err(WireError::Protocol(format!(
            "{what} format-code count {n} does not match value count {count}"
        ))),
    }
}

/// Text-encode one datum. Engine read paths detoast before values reach the
/// wire; a raw `Datum::External` still fails loudly rather than exposing its
/// physical pointer as user data.
pub fn encode_text(d: &Datum) -> Result<Vec<u8>> {
    let bytes = match d {
        Datum::Int4(v) => v.to_string().into_bytes(),
        Datum::Int8(v) => v.to_string().into_bytes(),
        Datum::Float8(v) => encode_float8_text(*v).into_bytes(),
        Datum::Timestamptz(us) => us.to_string().into_bytes(),
        Datum::Uuid(u) => u.to_string().into_bytes(),
        Datum::Text(s) | Datum::Jsonb(s) => s.as_bytes().to_vec(),
        Datum::Vector(v) => {
            let mut out = Vec::with_capacity(v.len() * 8 + 2);
            out.push(b'[');
            for (i, x) in v.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(x.to_string().as_bytes());
            }
            out.push(b']');
            out
        }
        Datum::Bytea(b) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut out = Vec::with_capacity(2 + b.len() * 2);
            out.extend_from_slice(b"\\x");
            for byte in b {
                out.push(HEX[(byte >> 4) as usize]);
                out.push(HEX[(byte & 0x0f) as usize]);
            }
            out
        }
        Datum::External(_) => {
            return Err(WireError::Encode(
                "TOASTed value cannot be resolved on the wire (M3)".to_string(),
            ))
        }
    };
    Ok(bytes)
}

/// Binary-encode one Stage C datum using network byte order.
pub fn encode_binary(d: &Datum) -> Result<Vec<u8>> {
    match d {
        Datum::Int4(v) => Ok(v.to_be_bytes().to_vec()),
        Datum::Int8(v) => Ok(v.to_be_bytes().to_vec()),
        Datum::Float8(v) => Ok(v.to_be_bytes().to_vec()),
        Datum::Text(s) => Ok(s.as_bytes().to_vec()),
        Datum::Vector(vector) => {
            let dim = u16::try_from(vector.len()).map_err(|_| {
                WireError::Encode(format!("vector dim {} exceeds u16", vector.len()))
            })?;
            let mut out = Vec::with_capacity(4 + vector.len() * 4);
            out.extend_from_slice(&dim.to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes());
            for value in vector {
                out.extend_from_slice(&value.to_be_bytes());
            }
            Ok(out)
        }
        other => Err(WireError::Encode(format!(
            "binary result encoding is not supported for {other:?}"
        ))),
    }
}

/// Encode one result value in the Bind-selected format.
pub fn encode_value(d: &Datum, format: i16) -> Result<Vec<u8>> {
    validate_format(format)?;
    match format {
        FORMAT_TEXT => encode_text(d),
        FORMAT_BINARY => encode_binary(d),
        _ => unreachable!("format validated above"),
    }
}

/// Decode one Bind parameter in its declared format. `None` is SQL NULL.
pub fn decode_parameter(ty: ColumnType, format: i16, bytes: Option<&[u8]>) -> Result<Value> {
    validate_format(format)?;
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let datum = match (ty, format) {
        (ColumnType::Int4, FORMAT_TEXT) => Datum::Int4(parse_text(bytes, "INT4")?),
        (ColumnType::Int8, FORMAT_TEXT) => Datum::Int8(parse_text(bytes, "INT8")?),
        (ColumnType::Float8, FORMAT_TEXT) => Datum::Float8(parse_float8_text(bytes)?),
        (ColumnType::Text, FORMAT_TEXT) => Datum::Text(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid_text("TEXT parameter is not UTF-8"))?
                .to_string(),
        ),
        (ColumnType::Jsonb, FORMAT_TEXT) => {
            let value = std::str::from_utf8(bytes)
                .map_err(|_| invalid_text("JSONB parameter is not UTF-8"))?;
            serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| invalid_text(format!("invalid JSONB parameter: {error}")))?;
            Datum::Jsonb(value.to_string())
        }
        (ColumnType::Vector(dim), FORMAT_TEXT) => {
            let vector = parse_vector_text(
                std::str::from_utf8(bytes)
                    .map_err(|_| invalid_text("VECTOR parameter is not UTF-8"))?,
            )?;
            Datum::Vector(validate_vector(vector, dim, "22P02")?)
        }
        (ColumnType::Int4, FORMAT_BINARY) => {
            Datum::Int4(i32::from_be_bytes(bytes.try_into().map_err(|_| {
                invalid_binary(format!(
                    "INT4 binary payload must be 4 bytes, got {}",
                    bytes.len()
                ))
            })?))
        }
        (ColumnType::Int8, FORMAT_BINARY) => {
            Datum::Int8(i64::from_be_bytes(bytes.try_into().map_err(|_| {
                invalid_binary(format!(
                    "INT8 binary payload must be 8 bytes, got {}",
                    bytes.len()
                ))
            })?))
        }
        (ColumnType::Float8, FORMAT_BINARY) => {
            Datum::Float8(f64::from_be_bytes(bytes.try_into().map_err(|_| {
                invalid_binary(format!(
                    "FLOAT8 binary payload must be 8 bytes, got {}",
                    bytes.len()
                ))
            })?))
        }
        (ColumnType::Text, FORMAT_BINARY) => Datum::Text(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid_binary("TEXT parameter is not UTF-8"))?
                .to_string(),
        ),
        (ColumnType::Vector(expected_dim), FORMAT_BINARY) => {
            if bytes.len() < 4 {
                return Err(invalid_binary(
                    "VECTOR binary payload is shorter than its header",
                ));
            }
            let dim = u16::from_be_bytes(bytes[0..2].try_into().unwrap());
            let unused = u16::from_be_bytes(bytes[2..4].try_into().unwrap());
            if unused != 0 {
                return Err(invalid_binary(format!(
                    "VECTOR binary unused field must be zero, got {unused}"
                )));
            }
            let expected_len = 4usize + dim as usize * 4;
            if bytes.len() != expected_len {
                return Err(invalid_binary(format!(
                    "VECTOR({dim}) binary payload must be {expected_len} bytes, got {}",
                    bytes.len()
                )));
            }
            let vector = bytes[4..]
                .chunks_exact(4)
                .map(|chunk| f32::from_be_bytes(chunk.try_into().unwrap()))
                .collect();
            Datum::Vector(validate_vector(vector, expected_dim, "22P03")?)
        }
        (other, _) => {
            return Err(WireError::Encode(format!(
                "Extended Query parameters are not supported for {other:?}"
            )))
        }
    };
    Ok(Some(datum))
}

/// Resolve a Parse parameter OID. VECTOR needs the dimension inferred from
/// its SQL context; zero is not a valid persisted vector dimension.
pub fn column_type_for_oid(oid: u32, vector_dim: u32) -> Result<ColumnType> {
    match oid {
        23 => Ok(ColumnType::Int4),
        20 => Ok(ColumnType::Int8),
        701 => Ok(ColumnType::Float8),
        25 => Ok(ColumnType::Text),
        114 => Ok(ColumnType::Jsonb),
        oid if u64::from(oid) == VECTOR_OID.raw().0 => {
            ColumnType::vector(vector_dim).map_err(|error| WireError::Encode(error.to_string()))
        }
        other => Err(WireError::Encode(format!(
            "unsupported parameter type OID {other}"
        ))),
    }
}

fn parse_text<T>(bytes: &[u8], what: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    let text = std::str::from_utf8(bytes)
        .map_err(|_| invalid_text(format!("{what} parameter is not UTF-8")))?;
    text.parse::<T>()
        .map_err(|_| invalid_text(format!("invalid {what} parameter {text:?}")))
}

fn parse_float8_text(bytes: &[u8]) -> Result<f64> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| invalid_text("FLOAT8 parameter is not UTF-8"))?;
    let value = match text {
        "NaN" => return Ok(f64::NAN),
        "Infinity" => return Ok(f64::INFINITY),
        "-Infinity" => return Ok(f64::NEG_INFINITY),
        _ => text
            .parse::<f64>()
            .map_err(|_| invalid_text(format!("invalid FLOAT8 parameter {text:?}")))?,
    };
    let mantissa = text.split(['e', 'E']).next().unwrap_or(text);
    let overflowed_to_infinity =
        value.is_infinite() && text.bytes().any(|byte| byte.is_ascii_digit());
    let underflowed_to_zero =
        value == 0.0 && mantissa.bytes().any(|byte| matches!(byte, b'1'..=b'9'));
    if overflowed_to_infinity || underflowed_to_zero {
        return Err(value_error(
            "22003",
            format!("FLOAT8 parameter {text:?} is out of range"),
        ));
    }
    Ok(value)
}

fn parse_vector_text(text: &str) -> Result<Vec<f32>> {
    let text = text.trim();
    let inner = text
        .strip_prefix('[')
        .and_then(|text| text.strip_suffix(']'))
        .ok_or_else(|| {
            invalid_text(format!(
                "VECTOR parameter must use pgvector text form '[...]', got {text:?}"
            ))
        })?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|component| {
            let component = component.trim();
            if component.is_empty() {
                return Err(invalid_text("VECTOR parameter contains an empty component"));
            }
            component
                .parse::<f32>()
                .map_err(|_| invalid_text(format!("invalid VECTOR component {component:?}")))
        })
        .collect()
}

fn validate_vector(
    vector: Vec<f32>,
    expected_dim: u32,
    sqlstate: &'static str,
) -> Result<Vec<f32>> {
    ColumnType::vector(expected_dim).map_err(|error| value_error(sqlstate, error.to_string()))?;
    if vector.len() != expected_dim as usize {
        return Err(value_error(
            sqlstate,
            format!(
                "VECTOR parameter has {} components, expected {expected_dim}",
                vector.len()
            ),
        ));
    }
    if let Some((index, _)) = vector
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(value_error(
            sqlstate,
            format!("VECTOR component {index} is not finite"),
        ));
    }
    Ok(vector)
}

/// PostgreSQL 16 default float8 text notation: shortest-roundtrip digits,
/// fixed notation for exponents -4..14, scientific notation elsewhere.
fn encode_float8_text(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value == f64::INFINITY {
        return "Infinity".to_string();
    }
    if value == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    let abs = value.abs();
    if abs != 0.0 && !(1e-4..1e15).contains(&abs) {
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific
            .split_once('e')
            .expect("finite LowerExp output contains e");
        let exponent: i32 = exponent
            .parse()
            .expect("finite LowerExp exponent is an integer");
        format!("{mantissa}e{exponent:+03}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_uses_oid_114_and_text_roundtrip() {
        assert_eq!(wire_type(ColumnType::Jsonb).oid, 114);
        assert_eq!(column_type_for_oid(114, 0).unwrap(), ColumnType::Jsonb);
        let original = "{ \"z\": 1, \"a\": true }";
        let datum = Datum::Jsonb(original.to_string());
        assert_eq!(encode_text(&datum).unwrap(), original.as_bytes());
        assert_eq!(column_type_of(&datum), ColumnType::Jsonb);
        assert_eq!(
            decode_parameter(ColumnType::Jsonb, FORMAT_TEXT, Some(original.as_bytes())).unwrap(),
            Some(datum)
        );
        assert!(decode_parameter(ColumnType::Jsonb, FORMAT_TEXT, Some(b"not json")).is_err());
        assert!(decode_parameter(ColumnType::Jsonb, FORMAT_BINARY, Some(b"{}")).is_err());
        assert!(encode_binary(&Datum::Jsonb("{}".to_string())).is_err());
    }
}
