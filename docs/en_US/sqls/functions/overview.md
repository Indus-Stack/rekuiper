# Functions

> [!NOTE]
> **Verification Status**: Tested and verified against the `rekuiper` runtime with streaming MQTT payloads and live assertions on **2026-10-14 15:00:00 UTC**.  
> **Scorecard**: **221 Verified (100.0%), 0 Unsupported, 0 Divergent across 221 evaluated functions**:
>
> | Category | Document | Total | Verified | Unsupported | Divergent/Broken | Parity Rate |
> | :--- | :--- | :---: | :---: | :---: | :---: | :---: |
> | **Math** | [mathematical_functions.md](./mathematical_functions.md) | 35 | 35 | 0 | 0 | **100%** |
> | **String** | [string_functions.md](./string_functions.md) | 26 | 26 | 0 | 0 | **100%** |
> | **Array** | [array_functions.md](./array_functions.md) | 32 | 32 | 0 | 0 | **100%** |
> | **Aggregate** | [aggregate_functions.md](./aggregate_functions.md) | 21 | 21 | 0 | 0 | **100%** |
> | **JSON** | [json_functions.md](./json_functions.md) | 5 | 5 | 0 | 0 | **100%** |
> | **Hashing** | [hashing_functions.md](./hashing_functions.md) | 6 | 6 | 0 | 0 | **100%** |
> | **Window** | [window_functions.md](./window_functions.md) | 1 | 1 | 0 | 0 | **100%** |
> | **Other** | [other_functions.md](./other_functions.md) | 18 | 18 | 0 | 0 | **100%** |
> | **DateTime** | [datetime_functions.md](./datetime_functions.md) | 30 | 30 | 0 | 0 | **100%** |
> | **Analytic** | [analytic_functions.md](./analytic_functions.md) | 18 | 18 | 0 | 0 | **100%** |
> | **Transform** | [transform_functions.md](./transform_functions.md) | 12 | 12 | 0 | 0 | **100%** |
> | **Object** | [object_functions.md](./object_functions.md) | 10 | 10 | 0 | 0 | **100%** |
> | **Vector** | [vector_functions.md](./vector_functions.md) | 4 | 4 | 0 | 0 | **100%** |
> | **Multi-Row** | [multi_row_functions.md](./multi_row_functions.md) | 2 | 2 | 0 | 0 | **100%** |
> | **Multi-Column** | [multi_column_functions.md](./multi_column_functions.md) | 1 | 1 | 0 | 0 | **100%** |
> | **Custom** | [custom_functions.md](./custom_functions.md) | Ext | Ext | Ext | 0 | *Plugin-Dependent* |
> | **TOTAL** | | **221** | **221** | **0** | **0** | **100.0%** |

## Built-in Scalar and Aggregate Functions

- [Aggregate Functions](./aggregate_functions.md)
- [Mathematical Functions](./mathematical_functions.md)
- [Vector Functions](./vector_functions.md)
- [String Functions](./string_functions.md)
- [Array Functions](./array_functions.md)
- [Object Functions](./object_functions.md)
- [Hashing Functions](./hashing_functions.md)
- [Transformation Functions](./transform_functions.md)
- [JSON Functions](./json_functions.md)
- [Date and Time Functions](./datetime_functions.md)
- [Other Functions](./other_functions.md)

## Advanced Stream Functions

- [Analytic Functions](./analytic_functions.md)
- [Multi-Row Functions](./multi_row_functions.md)
- [Multi-Column Functions](./multi_column_functions.md)
- [Window Functions](./window_functions.md)

## Plugin Extensions

rekuiper also provides functions through optional external plugins. Install the relevant plugins before invoking custom functions:

- [Custom Functions](./custom_functions.md)
