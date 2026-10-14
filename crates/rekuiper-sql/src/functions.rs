//! Built-in SQL function catalog for metadata discovery.
//!
//! [`builtin_function_metadata`] lists every scalar, aggregate, analytic and
//! system function the evaluator implements, with the arity each
//! implementation actually enforces. `GET /metadata/functions` serves this
//! table plus any registered plugin functions.

/// Static descriptor for one built-in function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub aggregate: bool,
    pub arity: &'static str,
    pub example: &'static str,
}

macro_rules! meta {
    ($name:literal, $cat:literal, $desc:literal, $agg:literal, $arity:literal, $ex:literal) => {
        FunctionMeta {
            name: $name,
            category: $cat,
            description: $desc,
            aggregate: $agg,
            arity: $arity,
            example: $ex,
        }
    };
}

/// Every built-in function (221 entries), mirroring the evaluator dispatch.
pub fn builtin_function_metadata() -> &'static [FunctionMeta] {
    static CATALOG: &[FunctionMeta] = &[
        // Math, trigonometry & bitwise (35).
        meta!(
            "abs",
            "math",
            "Absolute value of a number.",
            false,
            "1",
            "abs(-3)"
        ),
        meta!(
            "ceil",
            "math",
            "Smallest integer not less than the value.",
            false,
            "1",
            "ceil(1.2)"
        ),
        meta!(
            "ceiling",
            "math",
            "Alias of ceil.",
            false,
            "1",
            "ceiling(1.2)"
        ),
        meta!(
            "floor",
            "math",
            "Largest integer not greater than the value.",
            false,
            "1",
            "floor(1.8)"
        ),
        meta!(
            "round",
            "math",
            "Round to the given decimals (default 0).",
            false,
            "1-2",
            "round(1.235, 2)"
        ),
        meta!(
            "sqrt",
            "math",
            "Square root; Null for negatives.",
            false,
            "1",
            "sqrt(9)"
        ),
        meta!(
            "power",
            "math",
            "Raise x to the power y.",
            false,
            "2",
            "power(2, 10)"
        ),
        meta!("pow", "math", "Alias of power.", false, "2", "pow(2, 10)"),
        meta!("sin", "math", "Sine of radians.", false, "1", "sin(0)"),
        meta!("cos", "math", "Cosine of radians.", false, "1", "cos(0)"),
        meta!("tan", "math", "Tangent of radians.", false, "1", "tan(0)"),
        meta!(
            "asin",
            "math",
            "Arc sine; Null outside [-1, 1].",
            false,
            "1",
            "asin(1)"
        ),
        meta!(
            "acos",
            "math",
            "Arc cosine; Null outside [-1, 1].",
            false,
            "1",
            "acos(1)"
        ),
        meta!("atan", "math", "Arc tangent.", false, "1", "atan(1)"),
        meta!(
            "atan2",
            "math",
            "Arc tangent of y/x with quadrant correction.",
            false,
            "2",
            "atan2(1, 1)"
        ),
        meta!(
            "exp",
            "math",
            "e raised to the value.",
            false,
            "1",
            "exp(1)"
        ),
        meta!(
            "ln",
            "math",
            "Natural or base-b logarithm.",
            false,
            "1-2",
            "ln(10)"
        ),
        meta!(
            "log",
            "math",
            "Natural or base-b logarithm.",
            false,
            "1-2",
            "log(8, 2)"
        ),
        meta!("log2", "math", "Base-2 logarithm.", false, "1", "log2(8)"),
        meta!(
            "log10",
            "math",
            "Base-10 logarithm.",
            false,
            "1",
            "log10(100)"
        ),
        meta!(
            "sign",
            "math",
            "Sign of the value: -1, 0 or 1.",
            false,
            "1",
            "sign(-5)"
        ),
        meta!(
            "mod",
            "math",
            "Remainder of x divided by y.",
            false,
            "2",
            "mod(7, 3)"
        ),
        meta!("cosh", "math", "Hyperbolic cosine.", false, "1", "cosh(0)"),
        meta!("sinh", "math", "Hyperbolic sine.", false, "1", "sinh(0)"),
        meta!("tanh", "math", "Hyperbolic tangent.", false, "1", "tanh(0)"),
        meta!("cot", "math", "Cotangent of radians.", false, "1", "cot(1)"),
        meta!(
            "radians",
            "math",
            "Convert degrees to radians.",
            false,
            "1",
            "radians(180)"
        ),
        meta!(
            "degrees",
            "math",
            "Convert radians to degrees.",
            false,
            "1",
            "degrees(3.14159)"
        ),
        meta!(
            "bitand",
            "math",
            "Bitwise AND of two integers.",
            false,
            "2",
            "bitand(6, 3)"
        ),
        meta!(
            "bitor",
            "math",
            "Bitwise OR of two integers.",
            false,
            "2",
            "bitor(6, 3)"
        ),
        meta!(
            "bitxor",
            "math",
            "Bitwise XOR of two integers.",
            false,
            "2",
            "bitxor(6, 3)"
        ),
        meta!(
            "bitnot",
            "math",
            "Bitwise NOT of an integer.",
            false,
            "1",
            "bitnot(6)"
        ),
        meta!("pi", "math", "The constant pi.", false, "0", "pi()"),
        meta!(
            "rand",
            "math",
            "Random float in [0, 1).",
            false,
            "0",
            "rand()"
        ),
        meta!(
            "conv",
            "math",
            "Convert a number string between radix 2-36 bases.",
            false,
            "3",
            "conv('ff', 16, 10)"
        ),
        // String (16).
        meta!(
            "concat",
            "string",
            "Concatenate all arguments into one string.",
            false,
            "1+",
            "concat('a', 'b')"
        ),
        meta!(
            "lower",
            "string",
            "Lowercase the string.",
            false,
            "1",
            "lower('AB')"
        ),
        meta!(
            "upper",
            "string",
            "Uppercase the string.",
            false,
            "1",
            "upper('ab')"
        ),
        meta!(
            "length",
            "string",
            "Character length of the string.",
            false,
            "1",
            "length('abc')"
        ),
        meta!(
            "trim",
            "string",
            "Strip leading and trailing whitespace.",
            false,
            "1",
            "trim(' a ')"
        ),
        meta!(
            "ltrim",
            "string",
            "Strip leading whitespace.",
            false,
            "1",
            "ltrim(' a ')"
        ),
        meta!(
            "rtrim",
            "string",
            "Strip trailing whitespace.",
            false,
            "1",
            "rtrim(' a ')"
        ),
        meta!(
            "lpad",
            "string",
            "Left-pad to length with an optional pad string.",
            false,
            "2-3",
            "lpad('ab', 5, '0')"
        ),
        meta!(
            "rpad",
            "string",
            "Right-pad to length with an optional pad string.",
            false,
            "2-3",
            "rpad('ab', 5, '0')"
        ),
        meta!(
            "replace",
            "string",
            "Replace all occurrences of a substring.",
            false,
            "3",
            "replace('aaa', 'a', 'b')"
        ),
        meta!(
            "split",
            "string",
            "Split a string on a separator into an array.",
            false,
            "2",
            "split('a,b', ',')"
        ),
        meta!(
            "reverse",
            "string",
            "Reverse the characters of a string.",
            false,
            "1",
            "reverse('abc')"
        ),
        meta!(
            "substr",
            "string",
            "Substring with 1-based start and optional length.",
            false,
            "2-3",
            "substr('abcdef', 2, 3)"
        ),
        meta!(
            "substring",
            "string",
            "Alias of substr.",
            false,
            "2-3",
            "substring('abcdef', 2, 3)"
        ),
        meta!(
            "startswith",
            "string",
            "Whether the string starts with the prefix.",
            false,
            "2",
            "startswith('abc', 'a')"
        ),
        meta!(
            "endswith",
            "string",
            "Whether the string ends with the suffix.",
            false,
            "2",
            "endswith('abc', 'c')"
        ),
        meta!(
            "format_time",
            "string",
            "Format a datetime or epoch millis with a pattern.",
            false,
            "2",
            "format_time(now(), 'yyyy-MM-dd HH:mm:ss')"
        ),
        meta!(
            "indexof",
            "string",
            "0-based index of first substring occurrence; -1 if not found.",
            false,
            "2",
            "indexof('hello', 'll')"
        ),
        meta!(
            "format",
            "string",
            "Format number with decimal places and thousands grouping.",
            false,
            "2-3",
            "format(1234.567, 2)"
        ),
        // Array & object (42).
        meta!(
            "array_contains",
            "array",
            "Whether the array contains the value.",
            false,
            "2",
            "array_contains([1, 2], 2)"
        ),
        meta!(
            "array_join",
            "array",
            "Join array elements with an optional separator.",
            false,
            "1-2",
            "array_join([1, 2], ',')"
        ),
        meta!(
            "keys",
            "array",
            "Keys of an object as an array.",
            false,
            "1",
            "keys(obj)"
        ),
        meta!(
            "values",
            "array",
            "Values of an object as an array.",
            false,
            "1",
            "values(obj)"
        ),
        meta!(
            "object",
            "array",
            "Construct an object from an array of keys and an array of values.",
            false,
            "2",
            "object(['a', 'b'], [1, 2])"
        ),
        meta!(
            "zip",
            "array",
            "Construct an object from an array of key-value pairs.",
            false,
            "1",
            "zip([['a', 1], ['b', 2]])"
        ),
        meta!(
            "items",
            "array",
            "Return an array of key-value pair arrays from an object.",
            false,
            "1",
            "items(obj)"
        ),
        meta!(
            "object_construct",
            "array",
            "Build an object from key/value pairs.",
            false,
            "0+",
            "object_construct('a', 1)"
        ),
        meta!(
            "object_concat",
            "array",
            "Merge objects left to right.",
            false,
            "2+",
            "object_concat(o1, o2)"
        ),
        meta!(
            "erase",
            "array",
            "Remove keys from an object.",
            false,
            "2+",
            "erase(obj, 'a')"
        ),
        meta!(
            "object_erase",
            "array",
            "Alias of erase.",
            false,
            "2+",
            "object_erase(obj, 'a')"
        ),
        meta!(
            "object_pick",
            "array",
            "Keep only the listed keys of an object.",
            false,
            "2+",
            "object_pick(obj, 'a')"
        ),
        meta!(
            "obj_to_kvpair_array",
            "array",
            "Object to an array of {key, value} pairs.",
            false,
            "1",
            "obj_to_kvpair_array(obj)"
        ),
        meta!(
            "object_to_kvpair_array",
            "array",
            "Alias of obj_to_kvpair_array.",
            false,
            "1",
            "object_to_kvpair_array(obj)"
        ),
        meta!(
            "to_json",
            "array",
            "Serialize a value to a JSON string.",
            false,
            "1",
            "to_json(obj)"
        ),
        meta!(
            "tojson",
            "array",
            "Alias of to_json.",
            false,
            "1",
            "tojson(obj)"
        ),
        meta!(
            "parse_json",
            "array",
            "Parse a JSON string into a value.",
            false,
            "1",
            "parse_json('{\"a\":1}')"
        ),
        meta!(
            "parsejson",
            "array",
            "Alias of parse_json.",
            false,
            "1",
            "parsejson('{\"a\":1}')"
        ),
        meta!(
            "json_parse",
            "array",
            "Alias of parse_json.",
            false,
            "1",
            "json_parse('{\"a\":1}')"
        ),
        meta!(
            "array_create",
            "array",
            "Create an array from the arguments.",
            false,
            "0+",
            "array_create(1, 2)"
        ),
        meta!(
            "array_position",
            "array",
            "1-based position of a value, or Null.",
            false,
            "2",
            "array_position([1, 2], 2)"
        ),
        meta!(
            "array_positions",
            "array",
            "All 0-based index positions of a value in an array.",
            false,
            "2",
            "array_positions([1, 2, 2, 3, 2], 2)"
        ),
        meta!(
            "array_length",
            "array",
            "Number of elements in the array.",
            false,
            "1",
            "array_length([1, 2])"
        ),
        meta!(
            "array_slice",
            "array",
            "Slice with 1-based inclusive bounds.",
            false,
            "3",
            "array_slice([1, 2, 3], 1, 2)"
        ),
        meta!(
            "array_concat",
            "array",
            "Concatenate two arrays.",
            false,
            "2",
            "array_concat([1], [2])"
        ),
        meta!(
            "deduplicate",
            "array",
            "Remove duplicate array elements.",
            false,
            "1",
            "deduplicate([1, 1, 2])"
        ),
        meta!(
            "cardinality",
            "array",
            "Number of elements in an array.",
            false,
            "1",
            "cardinality([1, 2])"
        ),
        meta!(
            "array_cardinality",
            "array",
            "Alias of cardinality.",
            false,
            "1",
            "array_cardinality([1, 2])"
        ),
        meta!(
            "element_at",
            "array",
            "Element at 1-based index (negatives from end).",
            false,
            "2",
            "element_at([1, 2], -1)"
        ),
        meta!(
            "array_contains_any",
            "array",
            "Whether the array holds any listed value.",
            false,
            "2",
            "array_contains_any([1, 2], [2, 3])"
        ),
        meta!(
            "array_remove",
            "array",
            "Remove all occurrences of a value.",
            false,
            "2",
            "array_remove([1, 2, 1], 1)"
        ),
        meta!(
            "array_distinct",
            "array",
            "Distinct array elements.",
            false,
            "1",
            "array_distinct([1, 1, 2])"
        ),
        meta!(
            "array_intersect",
            "array",
            "Elements present in both arrays.",
            false,
            "2",
            "array_intersect([1, 2], [2, 3])"
        ),
        meta!(
            "array_union",
            "array",
            "Distinct union of two arrays.",
            false,
            "2",
            "array_union([1, 2], [2, 3])"
        ),
        meta!(
            "array_except",
            "array",
            "Elements of the first array missing from the second.",
            false,
            "2",
            "array_except([1, 2], [2])"
        ),
        meta!(
            "array_max",
            "array",
            "Maximum array element.",
            false,
            "1",
            "array_max([1, 2])"
        ),
        meta!(
            "array_min",
            "array",
            "Minimum array element.",
            false,
            "1",
            "array_min([1, 2])"
        ),
        meta!(
            "array_avg",
            "array",
            "Average of numeric array elements.",
            false,
            "1",
            "array_avg([1, 2])"
        ),
        meta!(
            "array_flatten",
            "array",
            "Flatten one level of nesting.",
            false,
            "1",
            "array_flatten([[1], [2]])"
        ),
        meta!(
            "array_sort",
            "array",
            "Sort array elements ascending.",
            false,
            "1",
            "array_sort([2, 1])"
        ),
        meta!(
            "repeat",
            "array",
            "Repeat a value n times into an array.",
            false,
            "2",
            "repeat('a', 3)"
        ),
        meta!(
            "sequence",
            "array",
            "Integer sequence from start to stop with optional step.",
            false,
            "2-3",
            "sequence(1, 5)"
        ),
        meta!(
            "kvpair_array_to_obj",
            "array",
            "Array of {key, value} pairs back to an object.",
            false,
            "1",
            "kvpair_array_to_obj(pairs)"
        ),
        meta!(
            "array_last_position",
            "array",
            "1-based index of last occurrence of value; 0 if absent.",
            false,
            "2",
            "array_last_position([1, 2, 1], 1)"
        ),
        meta!(
            "array_shuffle",
            "array",
            "Randomly permute the elements of an array.",
            false,
            "1",
            "array_shuffle([1, 2, 3])"
        ),
        meta!(
            "array_map",
            "array",
            "Apply a scalar function to each array element.",
            false,
            "2",
            "array_map('abs', [-1, 2])"
        ),
        // DateTime (28).
        meta!(
            "now",
            "datetime",
            "Current UTC timestamp with optional format or precision.",
            false,
            "0-1",
            "now()"
        ),
        meta!(
            "current_timestamp",
            "datetime",
            "Current timestamp with optional format or precision.",
            false,
            "0-1",
            "current_timestamp()"
        ),
        meta!(
            "local_timestamp",
            "datetime",
            "Current local timestamp with optional format or precision.",
            false,
            "0-1",
            "local_timestamp()"
        ),
        meta!(
            "current_date",
            "datetime",
            "Current date.",
            false,
            "0",
            "current_date()"
        ),
        meta!(
            "cur_date",
            "datetime",
            "Alias of current_date.",
            false,
            "0",
            "cur_date()"
        ),
        meta!(
            "current_time",
            "datetime",
            "Current time.",
            false,
            "0",
            "current_time()"
        ),
        meta!(
            "cur_time",
            "datetime",
            "Alias of current_time.",
            false,
            "0",
            "cur_time()"
        ),
        meta!(
            "local_time",
            "datetime",
            "Current local time.",
            false,
            "0",
            "local_time()"
        ),
        meta!(
            "format_date",
            "datetime",
            "Format epoch millis or RFC3339 with a pattern.",
            false,
            "2",
            "format_date(ts, 'yyyy-MM-dd')"
        ),
        meta!(
            "from_unix_time",
            "datetime",
            "Epoch seconds to timestamp with optional format.",
            false,
            "1-2",
            "from_unix_time(1700000000)"
        ),
        meta!(
            "date_parse",
            "datetime",
            "Parse a date string with a format pattern.",
            false,
            "2",
            "date_parse('2024-01-02', 'yyyy-MM-dd')"
        ),
        meta!(
            "date_add",
            "datetime",
            "Add n units to a timestamp.",
            false,
            "3",
            "date_add('day', 1, ts)"
        ),
        meta!(
            "date_diff",
            "datetime",
            "Difference of two timestamps in units.",
            false,
            "2-3",
            "date_diff('day', t1, t2)"
        ),
        meta!(
            "date_calc",
            "datetime",
            "Calculates date based on date and duration string.",
            false,
            "2",
            "date_calc('2019-01-01', '1h30m')"
        ),
        meta!(
            "convert_tz",
            "datetime",
            "Converts a timestamp to target timezone.",
            false,
            "2-3",
            "convert_tz(ts, 'Asia/Shanghai')"
        ),
        meta!(
            "year",
            "datetime",
            "Year of a timestamp.",
            false,
            "1",
            "year(ts)"
        ),
        meta!(
            "month",
            "datetime",
            "Month of a timestamp (1-12).",
            false,
            "1",
            "month(ts)"
        ),
        meta!(
            "day",
            "datetime",
            "Day of month of a timestamp.",
            false,
            "1",
            "day(ts)"
        ),
        meta!(
            "day_of_week",
            "datetime",
            "Weekday 1=Sunday through 7=Saturday.",
            false,
            "1",
            "day_of_week(ts)"
        ),
        meta!(
            "day_of_month",
            "datetime",
            "Day of month of a timestamp.",
            false,
            "1",
            "day_of_month(ts)"
        ),
        meta!(
            "day_of_year",
            "datetime",
            "Day of year of a timestamp.",
            false,
            "1",
            "day_of_year(ts)"
        ),
        meta!(
            "day_name",
            "datetime",
            "Weekday name of a timestamp.",
            false,
            "1",
            "day_name(ts)"
        ),
        meta!(
            "month_name",
            "datetime",
            "Month name of a timestamp.",
            false,
            "1",
            "month_name(ts)"
        ),
        meta!(
            "microsecond",
            "datetime",
            "Microsecond part of a timestamp.",
            false,
            "1",
            "microsecond(ts)"
        ),
        meta!(
            "last_day",
            "datetime",
            "Last day of the timestamp month.",
            false,
            "1",
            "last_day(ts)"
        ),
        meta!(
            "to_seconds",
            "datetime",
            "Days since year 0 as seconds.",
            false,
            "1",
            "to_seconds(ts)"
        ),
        meta!(
            "from_days",
            "datetime",
            "Days since year 0 back to a date.",
            false,
            "1",
            "from_days(739000)"
        ),
        meta!(
            "hour",
            "datetime",
            "Hour of a timestamp.",
            false,
            "1",
            "hour(ts)"
        ),
        meta!(
            "minute",
            "datetime",
            "Minute of a timestamp.",
            false,
            "1",
            "minute(ts)"
        ),
        meta!(
            "second",
            "datetime",
            "Second of a timestamp.",
            false,
            "1",
            "second(ts)"
        ),
        // JSON path (4).
        meta!(
            "json_path_query",
            "json",
            "All values matching a JSON path.",
            false,
            "2",
            "json_path_query(doc, '$.a')"
        ),
        meta!(
            "json_path_query_first",
            "json",
            "First value matching a JSON path.",
            false,
            "2",
            "json_path_query_first(doc, '$.a')"
        ),
        meta!(
            "json_path_exists",
            "json",
            "Whether a JSON path matches anything.",
            false,
            "2",
            "json_path_exists(doc, '$.a')"
        ),
        meta!(
            "json_map",
            "json",
            "Build an object from key/value pairs.",
            false,
            "0+",
            "json_map('a', 1)"
        ),
        // Crypto, encoding & regex (19).
        meta!(
            "md5",
            "crypto",
            "MD5 hex digest of a string.",
            false,
            "1",
            "md5('abc')"
        ),
        meta!(
            "sha256",
            "crypto",
            "SHA-256 hex digest of a string.",
            false,
            "1",
            "sha256('abc')"
        ),
        meta!(
            "sha512",
            "crypto",
            "SHA-512 hex digest of a string.",
            false,
            "1",
            "sha512('abc')"
        ),
        meta!(
            "sha1",
            "crypto",
            "SHA-1 hex digest of a string.",
            false,
            "1",
            "sha1('abc')"
        ),
        meta!(
            "sha384",
            "crypto",
            "SHA-384 hex digest of a string.",
            false,
            "1",
            "sha384('abc')"
        ),
        meta!(
            "crc32",
            "crypto",
            "IEEE CRC-32 checksum of a string.",
            false,
            "1",
            "crc32('abc')"
        ),
        meta!(
            "regexp_matches",
            "crypto",
            "Whether the text matches the regex.",
            false,
            "2",
            "regexp_matches('abc', '^a')"
        ),
        meta!(
            "regexp_replace",
            "crypto",
            "Replace regex matches in text.",
            false,
            "3",
            "regexp_replace('abc', 'b', 'x')"
        ),
        meta!(
            "regexp_substring",
            "crypto",
            "First regex match (group 1 preferred).",
            false,
            "2",
            "regexp_substring('abc123', '[0-9]+')"
        ),
        meta!(
            "regexp_substr",
            "crypto",
            "First regex match (group 1 preferred).",
            false,
            "2",
            "regexp_substr('abc123', '[0-9]+')"
        ),
        meta!(
            "split_value",
            "crypto",
            "0-based split segment at an index.",
            false,
            "3",
            "split_value('a,b', ',', 0)"
        ),
        meta!(
            "numbytes",
            "crypto",
            "UTF-8 byte length of a string.",
            false,
            "1",
            "numbytes('abc')"
        ),
        meta!(
            "chr",
            "crypto",
            "Unicode character for a code point.",
            false,
            "1",
            "chr(65)"
        ),
        meta!(
            "trunc",
            "crypto",
            "Truncate toward zero with optional decimals.",
            false,
            "1-2",
            "trunc(1.99)"
        ),
        meta!(
            "hex2dec",
            "crypto",
            "Hexadecimal string to decimal.",
            false,
            "1",
            "hex2dec('ff')"
        ),
        meta!(
            "dec2hex",
            "crypto",
            "Decimal to 0x-prefixed lowercase hex.",
            false,
            "1",
            "dec2hex(255)"
        ),
        meta!(
            "encode",
            "crypto",
            "Encode a value with the named method.",
            false,
            "2",
            "encode('abc', 'base64')"
        ),
        meta!(
            "base64_encode",
            "crypto",
            "Base64-encode a value.",
            false,
            "1",
            "base64_encode('abc')"
        ),
        meta!(
            "decode",
            "crypto",
            "Decode text with the named method.",
            false,
            "2",
            "decode('YWJj', 'base64')"
        ),
        meta!(
            "base64_decode",
            "crypto",
            "Base64-decode text.",
            false,
            "1",
            "base64_decode('YWJj')"
        ),
        meta!(
            "compress",
            "crypto",
            "Compress input with zlib, gzip, flate, or zstd.",
            false,
            "2",
            "compress('abc', 'zlib')"
        ),
        meta!(
            "decompress",
            "crypto",
            "Decompress input with zlib, gzip, flate, or zstd.",
            false,
            "2",
            "decompress('eJzT0yMAAGUAAM8=', 'zlib')"
        ),
        // Conversion & utility (9).
        meta!(
            "cast",
            "conversion",
            "Convert a value to the target type.",
            false,
            "1-2",
            "cast(x, 'bigint')"
        ),
        meta!(
            "delay",
            "conversion",
            "Delay rule execution for the specified milliseconds and return value.",
            false,
            "2",
            "delay(10, 'ok')"
        ),
        meta!(
            "unnest",
            "conversion",
            "Expands an array into multiple rows.",
            false,
            "1",
            "unnest(arr)"
        ),
        meta!(
            "extract",
            "conversion",
            "Expands a map object onto the current row.",
            false,
            "1",
            "extract(obj)"
        ),
        meta!(
            "coalesce",
            "conversion",
            "First non-null argument.",
            false,
            "1+",
            "coalesce(a, b, 0)"
        ),
        meta!(
            "isnan",
            "conversion",
            "Whether the value is NaN.",
            false,
            "1",
            "isnan(x)"
        ),
        meta!(
            "isnumeric",
            "conversion",
            "Whether the value is numeric.",
            false,
            "1",
            "isnumeric(x)"
        ),
        meta!(
            "nvl",
            "conversion",
            "The value, or a default when null.",
            false,
            "2",
            "nvl(a, 0)"
        ),
        meta!(
            "isnull",
            "conversion",
            "Whether the value is null.",
            false,
            "1",
            "isnull(a)"
        ),
        meta!(
            "tstamp",
            "conversion",
            "Current epoch milliseconds.",
            false,
            "0",
            "tstamp()"
        ),
        meta!(
            "uuid",
            "conversion",
            "New random UUID string.",
            false,
            "0",
            "uuid()"
        ),
        meta!(
            "newuuid",
            "conversion",
            "Alias of uuid.",
            false,
            "0",
            "newuuid()"
        ),
        // Aggregates (18).
        meta!(
            "avg",
            "aggregate",
            "Average of column values.",
            true,
            "1",
            "avg(temp)"
        ),
        meta!(
            "collect",
            "aggregate",
            "Collect column values into an array.",
            true,
            "1",
            "collect(*)"
        ),
        meta!(
            "count",
            "aggregate",
            "Row or non-null value count.",
            true,
            "1",
            "count(*)"
        ),
        meta!(
            "last_value",
            "aggregate",
            "Last non-null value with optional null skipping.",
            true,
            "1-2",
            "last_value(temp)"
        ),
        meta!(
            "latest",
            "aggregate",
            "Most recent column value with optional default.",
            true,
            "1-2",
            "latest(temp)"
        ),
        meta!(
            "lead",
            "aggregate",
            "Value n rows ahead with optional default and ignoreNull.",
            true,
            "1-4",
            "lead(temp, 1)"
        ),
        meta!(
            "max",
            "aggregate",
            "Maximum column value.",
            true,
            "1",
            "max(temp)"
        ),
        meta!(
            "median",
            "aggregate",
            "Median of column values.",
            true,
            "1",
            "median(temp)"
        ),
        meta!(
            "merge_agg",
            "aggregate",
            "Merge object values left to right.",
            true,
            "1",
            "merge_agg(obj)"
        ),
        meta!(
            "min",
            "aggregate",
            "Minimum column value.",
            true,
            "1",
            "min(temp)"
        ),
        meta!(
            "percentile",
            "aggregate",
            "Continuous percentile by interpolation.",
            true,
            "2",
            "percentile(temp, 0.9)"
        ),
        meta!(
            "percentile_cont",
            "aggregate",
            "Continuous percentile by interpolation.",
            true,
            "2",
            "percentile_cont(temp, 0.9)"
        ),
        meta!(
            "percentile_disc",
            "aggregate",
            "Discrete percentile value.",
            true,
            "2",
            "percentile_disc(temp, 0.9)"
        ),
        meta!(
            "row_number",
            "aggregate",
            "1-based row number in the batch.",
            true,
            "0",
            "row_number()"
        ),
        meta!(
            "stddev",
            "aggregate",
            "Population standard deviation.",
            true,
            "1",
            "stddev(temp)"
        ),
        meta!(
            "stddevs",
            "aggregate",
            "Sample standard deviation.",
            true,
            "1",
            "stddevs(temp)"
        ),
        meta!(
            "sum",
            "aggregate",
            "Sum of column values.",
            true,
            "1",
            "sum(temp)"
        ),
        meta!(
            "var",
            "aggregate",
            "Population variance.",
            true,
            "1",
            "var(temp)"
        ),
        meta!(
            "vars",
            "aggregate",
            "Sample variance.",
            true,
            "1",
            "vars(temp)"
        ),
        meta!(
            "last_agg_hit_count",
            "aggregate",
            "Accumulated trigger count of aggregate rule.",
            true,
            "0",
            "last_agg_hit_count()"
        ),
        meta!(
            "last_agg_hit_time",
            "aggregate",
            "Timestamp of last aggregate rule hit.",
            true,
            "0",
            "last_agg_hit_time()"
        ),
        // Analytic / stateful (11).
        meta!(
            "acc_avg",
            "analytic",
            "Running average over the stream.",
            false,
            "1",
            "acc_avg(temp)"
        ),
        meta!(
            "acc_count",
            "analytic",
            "Running count of non-null values.",
            false,
            "1",
            "acc_count(temp)"
        ),
        meta!(
            "acc_map_agg",
            "analytic",
            "Running map keyed by the first argument.",
            false,
            "2",
            "acc_map_agg(k, v)"
        ),
        meta!(
            "acc_max",
            "analytic",
            "Running maximum over the stream.",
            false,
            "1",
            "acc_max(temp)"
        ),
        meta!(
            "acc_max_by",
            "analytic",
            "Running value with the maximum key.",
            false,
            "2",
            "acc_max_by(ts, temp)"
        ),
        meta!(
            "acc_min",
            "analytic",
            "Running minimum over the stream.",
            false,
            "1",
            "acc_min(temp)"
        ),
        meta!(
            "acc_min_by",
            "analytic",
            "Running value with the minimum key.",
            false,
            "2",
            "acc_min_by(ts, temp)"
        ),
        meta!(
            "acc_sum",
            "analytic",
            "Running sum over the stream.",
            false,
            "1",
            "acc_sum(temp)"
        ),
        meta!(
            "changed_col",
            "analytic",
            "Current value when it changed, else Null.",
            false,
            "1",
            "changed_col(temp)"
        ),
        meta!(
            "had_changed",
            "analytic",
            "Whether the column value changed.",
            false,
            "1",
            "had_changed(temp)"
        ),
        meta!(
            "lag",
            "analytic",
            "Value n rows behind with optional default and ignoreNull.",
            false,
            "1-4",
            "lag(temp, 1)"
        ),
        meta!(
            "changed_cols",
            "analytic",
            "Columns that changed compared to previous emit.",
            false,
            "2+",
            "changed_cols('', true, col1)"
        ),
        meta!(
            "acc_collect",
            "analytic",
            "Collects non-nil expression results into an array.",
            false,
            "1",
            "acc_collect(val)"
        ),
        meta!(
            "acc_distinct_collect",
            "analytic",
            "Collects unique non-nil expression results into an array preserving order.",
            false,
            "1",
            "acc_distinct_collect(val)"
        ),
        meta!(
            "distinct_acc",
            "analytic",
            "Alias of acc_distinct_collect.",
            false,
            "1",
            "distinct_acc(val)"
        ),
        meta!(
            "sequence_match",
            "analytic",
            "Determines whether multiple pattern steps occurred in temporal order within max_span_ms.",
            false,
            "3+",
            "sequence_match(30000, temp > 80.0, pressure > 120.0)"
        ),
        meta!(
            "sequence_count",
            "analytic",
            "Counts the number of times multiple pattern steps occurred in temporal order within max_span_ms.",
            false,
            "3+",
            "sequence_count(30000, temp > 80.0, pressure > 120.0)"
        ),
        // Contextual / system (6).
        meta!(
            "event_time",
            "system",
            "Event timestamp of the current record.",
            false,
            "0",
            "event_time()"
        ),
        meta!(
            "meta",
            "system",
            "Message metadata by key.",
            false,
            "1",
            "meta('topic')"
        ),
        meta!(
            "mqtt",
            "system",
            "MQTT message field by key.",
            false,
            "1",
            "mqtt('topic')"
        ),
        meta!(
            "rule_id",
            "system",
            "Id of the executing rule.",
            false,
            "0",
            "rule_id()"
        ),
        meta!(
            "window_end",
            "system",
            "End bound of the current window.",
            false,
            "0",
            "window_end()"
        ),
        meta!(
            "window_start",
            "system",
            "Start bound of the current window.",
            false,
            "0",
            "window_start()"
        ),
        meta!(
            "rule_start",
            "system",
            "Rule start timestamp in int64 format.",
            false,
            "0",
            "rule_start()"
        ),
        meta!(
            "last_hit_count",
            "system",
            "Accumulated trigger count of continuous rule.",
            false,
            "0",
            "last_hit_count()"
        ),
        meta!(
            "last_hit_time",
            "system",
            "Timestamp of last continuous rule hit.",
            false,
            "0",
            "last_hit_time()"
        ),
        meta!(
            "get_keyed_state",
            "system",
            "Keyed state value from database with type casting and default.",
            false,
            "3",
            "get_keyed_state('k', 'string', '')"
        ),
        // Vector similarity & distance (4).
        meta!(
            "cosine_similarity",
            "vector",
            "Cosine similarity between two numerical vectors [-1.0, 1.0].",
            false,
            "2",
            "cosine_similarity([1.0, 0.0], [0.0, 1.0])"
        ),
        meta!(
            "vector_l2",
            "vector",
            "Euclidean L2 distance between two numerical vectors.",
            false,
            "2",
            "vector_l2([1.0, 2.0], [4.0, 6.0])"
        ),
        meta!(
            "vector_dot",
            "vector",
            "Dot product between two numerical vectors.",
            false,
            "2",
            "vector_dot([1.0, 2.0], [3.0, 4.0])"
        ),
        meta!(
            "vector_match",
            "vector",
            "Find top K nearest candidate vectors or objects by cosine similarity.",
            false,
            "2-3",
            "vector_match([1.0, 0.0], [[1.0, 0.0], [0.0, 1.0]], 1)"
        ),
        // WebAssembly function plugin (1).
        meta!(
            "wasm_run",
            "plugin",
            "Execute an exported scalar function from a loaded WebAssembly module.",
            false,
            "2+",
            "wasm_run('math', 'add', 10, 20)"
        ),
    ];
    CATALOG
}

/// Whether `name` is a built-in function (matched case-insensitively,
/// like SQL resolution).
pub fn is_builtin_function(name: &str) -> bool {
    static BUILTIN_SET: std::sync::LazyLock<std::collections::HashSet<&'static str>> =
        std::sync::LazyLock::new(|| builtin_function_metadata().iter().map(|m| m.name).collect());
    BUILTIN_SET.contains(name.to_ascii_lowercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn catalog_covers_all_builtins_exactly_once() {
        let catalog = builtin_function_metadata();
        assert_eq!(catalog.len(), 221, "catalog must list 221 functions");
        let mut seen = HashSet::new();
        for meta in catalog {
            assert!(
                seen.insert(meta.name),
                "duplicate catalog entry: {}",
                meta.name
            );
            assert!(
                !meta.description.is_empty(),
                "{} needs a description",
                meta.name
            );
            assert!(!meta.example.is_empty(), "{} needs an example", meta.name);
        }
    }
}
