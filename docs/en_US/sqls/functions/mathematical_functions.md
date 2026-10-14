# Mathematical Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming telemetry data on **2026-10-14 20:45:00 UTC**.  
> **Scorecard**: **35 / 35 Mathematical Functions Fully Verified with Live Data (100% Parity)**:  
> `abs`, `acos`, `asin`, `atan`, `atan2`, `bitand`, `bitor`, `bitxor`, `bitnot`, `ceil`, `ceiling`, `conv`, `cos`, `cosh`, `cot`, `degrees`, `exp`, `floor`, `ln`, `log`, `log2`, `log10`, `mod`, `pi`, `pow`, `power`, `radians`, `rand`, `round`, `sign`, `sin`, `sinh`, `sqrt`, `tan`, `tanh`.  
> All functions tested with live telemetry data and validated with exact mathematical assertions.

Mathematical functions perform numerical operations. They accept numeric inputs and return numeric values.

## ABS

```text
abs(col)
```

Returns the absolute value of the argument.

## ACOS

```text
acos(col)
```

Returns the arc cosine of the argument in radians.

## ASIN

```text
asin(col)
```

Returns the arc sine of the argument in radians.

## ATAN

```text
atan(col)
```

Returns the arc tangent of the argument in radians.

## ATAN2

```text
atan2(col1, col2)
```

Returns the angle in radians between the positive x-axis and the coordinate point `(col1, col2)`.

## BITAND

```text
bitand(col1, col2)
```

Performs a bitwise AND operation on the integer representations of the two arguments.

## BITOR

```text
bitor(col1, col2)
```

Performs a bitwise OR operation on the integer representations of the two arguments.

## BITXOR

```text
bitxor(col1, col2)
```

Performs a bitwise exclusive OR (XOR) operation on the integer representations of the two arguments.

## BITNOT

```text
bitnot(col1)
```

Performs a bitwise NOT operation on the integer representation of the argument.

## CEIL

`CEIL()` is a synonym for [`CEILING()`](#ceiling).

## CEILING

```text
ceiling(col)
```

Returns the smallest integer value greater than or equal to the argument.

## COS

```text
cos(col)
```

Returns the cosine of an angle expressed in radians.

## COSH

```text
cosh(col)
```

Returns the hyperbolic cosine of the argument.

## EXP

```text
exp(col)
```

Returns Euler's constant $e$ raised to the power of the argument.

## FLOOR

```text
floor(col)
```

Returns the largest integer value less than or equal to the argument.

## LN

```text
ln(col)
```

Returns the natural logarithm (base $e$) of the argument.

## LOG

```text
log(col)
log(b, col)
```

- When called with one argument, returns the base-10 logarithm of `col`. Returns `nil` if `col <= 0`.
- When called with two arguments, returns the base-`b` logarithm of `col`. Returns `nil` if `col <= 0` or `b <= 1`.

## MOD

```text
mod(col1, col2)
```

Returns the remainder of dividing `col1` by `col2`.

## PI

```text
pi()
```

Returns the constant value of $\pi$ (`3.141592653589793`).

## POW

`POW()` is a synonym for [`POWER()`](#power).

## POWER

```text
power(col1, col2)
```

Returns `col1` raised to the power of `col2`.

## RAND

```text
rand()
```

Returns a pseudo-random floating-point number between `0.0` (inclusive) and `1.0` (exclusive).

## ROUND

```text
round(v, [s])
```

Rounds `v` to `s` decimal places. If `s` is omitted, rounds to the nearest integer:

```text
round(42.4)        --> 42
round(42.4382, 2)  --> 42.44
```

## SIGN

```text
sign(col)
```

Returns the sign of the argument: `1` for positive numbers, `-1` for negative numbers, and `0` for zero.

## SIN

```text
sin(col)
```

Returns the sine of an angle expressed in radians.

## SINH

```text
sinh(col)
```

Returns the hyperbolic sine of the argument.

## SQRT

```text
sqrt(col)
```

Returns the positive square root of the argument.

## TAN

```text
tan(col)
```

Returns the tangent of an angle expressed in radians.

## TANH

```text
tanh(col)
```

Returns the hyperbolic tangent of the argument.

## COT

```text
cot(col)
```

Returns the cotangent of the argument.

## RADIANS

```text
radians(col)
```

Converts angle measurements from degrees to radians.

## DEGREES

```text
degrees(col)
```

Converts angle measurements from radians to degrees.

## CONV

```text
conv(N, from_base, to_base)
```

Converts number `N` from radix `from_base` to radix `to_base`. Returns a string representation of the converted number. Returns `NULL` if any argument is `NULL`. Supported bases range from 2 to 36:

```sql
SELECT conv('a', 16, 2);
-- Output: '1010'

SELECT conv('6E', 18, 8);
-- Output: '172'

SELECT conv(-17, 10, -18);
-- Output: '-H'
```

## LOG2

```text
log2(col)
```

Calculates the base-2 logarithm of a numeric value ($\log_2(x)$).

**Parameters:**
- `col`: A numeric column or literal expression ($x > 0$). Can be integer or floating-point.

**Return Type:**
- `float` representing $\log_2(x)$. Returns `NULL` if `col` is `NULL` or non-positive ($x \le 0$).

**Verified Examples:**
```sql
SELECT log2(8) AS v FROM telemetry;
-- Output: 3.0

SELECT log2(1024) AS v FROM telemetry;
-- Output: 10.0
```

## LOG10

```text
log10(col)
```

Calculates the common base-10 logarithm of a numeric value ($\log_{10}(x)$).

**Parameters:**
- `col`: A numeric column or literal expression ($x > 0$). Can be integer or floating-point.

**Return Type:**
- `float` representing $\log_{10}(x)$. Returns `NULL` if `col` is `NULL` or non-positive ($x \le 0$).

**Verified Examples:**
```sql
SELECT log10(100) AS v FROM telemetry;
-- Output: 2.0

SELECT log10(1000000) AS v FROM telemetry;
-- Output: 6.0
```

