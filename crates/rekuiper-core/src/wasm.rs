use anyhow::{bail, Context, Result};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use wasmi::{Config, Engine, Instance, Linker, Module, Store, Val};

/// Default instruction fuel budget per WASM UDF invocation.
const DEFAULT_WASM_FUEL_BUDGET: u64 = 1_000_000;

struct WasmRuntimeInstance {
    store: Store<()>,
    instance: Instance,
    params_buf: Vec<Val>,
}

/// Compiled WebAssembly module ready for isolated, sandboxed execution.
#[derive(Clone)]
pub struct WasmModule {
    engine: Engine,
    module: Module,
    runtime_pool: Arc<Mutex<Vec<WasmRuntimeInstance>>>,
}

impl WasmModule {
    fn instantiate_runtime(engine: &Engine, module: &Module) -> Result<WasmRuntimeInstance> {
        let mut store = Store::new(engine, ());
        store
            .set_fuel(DEFAULT_WASM_FUEL_BUDGET)
            .context("Failed to initialize WASM fuel budget")?;
        let linker = Linker::new(engine);
        let instance = linker
            .instantiate(&mut store, module)
            .context("Failed to instantiate WASM module")?
            .start(&mut store)
            .context("Failed to start WASM instance")?;
        Ok(WasmRuntimeInstance {
            store,
            instance,
            params_buf: Vec::with_capacity(8),
        })
    }

    /// Compiles and pre-instantiates a WebAssembly module from raw binary bytecode.
    pub fn from_bytes(wasm_bytes: &[u8]) -> Result<Self> {
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, wasm_bytes)
            .context("Failed to parse and validate WebAssembly module")?;
        let initial_instance = Self::instantiate_runtime(&engine, &module)?;
        Ok(Self {
            engine,
            module,
            runtime_pool: Arc::new(Mutex::new(vec![initial_instance])),
        })
    }

    /// Invokes an exported scalar function by name using a pooled instance and bounded fuel.
    pub fn call_function(&self, func_name: &str, args: &[Value]) -> Result<Value> {
        let mut rt = {
            let mut pool = self.runtime_pool.lock();
            pool.pop()
        }
        .map(Ok)
        .unwrap_or_else(|| Self::instantiate_runtime(&self.engine, &self.module))?;

        let exec_res = (|| -> Result<Value> {
            let WasmRuntimeInstance {
                store,
                instance,
                params_buf,
            } = &mut rt;

            store
                .set_fuel(DEFAULT_WASM_FUEL_BUDGET)
                .context("Failed to replenish WASM execution fuel")?;

            let func = instance.get_func(&*store, func_name).ok_or_else(|| {
                anyhow::anyhow!("Exported function '{}' not found in WASM module", func_name)
            })?;

            let func_type = func.ty(&*store);
            let param_types = func_type.params();

            if args.len() != param_types.len() {
                bail!(
                    "Function '{}' expects {} arguments, but received {}",
                    func_name,
                    param_types.len(),
                    args.len()
                );
            }

            params_buf.clear();
            for (idx, (arg, p_type)) in args.iter().zip(param_types).enumerate() {
                match p_type {
                    wasmi::core::ValType::I32 => {
                        let i = arg
                            .as_i64()
                            .or_else(|| arg.as_f64().map(|f| f as i64))
                            .unwrap_or(0) as i32;
                        params_buf.push(Val::I32(i));
                    }
                    wasmi::core::ValType::I64 => {
                        let i = arg
                            .as_i64()
                            .or_else(|| arg.as_f64().map(|f| f as i64))
                            .unwrap_or(0);
                        params_buf.push(Val::I64(i));
                    }
                    wasmi::core::ValType::F32 => {
                        let f = arg.as_f64().unwrap_or(0.0) as f32;
                        params_buf.push(Val::F32(f.into()));
                    }
                    wasmi::core::ValType::F64 => {
                        let f = arg.as_f64().unwrap_or(0.0);
                        params_buf.push(Val::F64(f.into()));
                    }
                    _ => bail!("Unsupported WASM parameter type for argument {}", idx),
                }
            }

            let mut results = vec![Val::I32(0); func_type.results().len()];
            func.call(store, params_buf, &mut results)
                .context("WASM execution trapped or encountered runtime error")?;

            if results.is_empty() {
                return Ok(Value::Null);
            }

            let res_val = match results[0] {
                Val::I32(i) => serde_json::json!(i),
                Val::I64(i) => serde_json::json!(i),
                Val::F32(f) => serde_json::json!(f.to_float()),
                Val::F64(f) => serde_json::json!(f.to_float()),
                _ => Value::Null,
            };

            Ok(res_val)
        })();

        if exec_res.is_ok() {
            let mut pool = self.runtime_pool.lock();
            if pool.len() < 16 {
                pool.push(rt);
            }
        }

        exec_res
    }

    /// List all exported functions in this WASM module.
    pub fn exported_functions(&self) -> Vec<String> {
        self.module
            .exports()
            .filter_map(|export| {
                if let wasmi::ExternType::Func(_) = export.ty() {
                    Some(export.name().to_string())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Global registry of loaded WASM function plugin modules.
#[derive(Clone, Default)]
pub struct WasmPluginRegistry {
    modules: Arc<RwLock<HashMap<String, Arc<WasmModule>>>>,
}

impl WasmPluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a WASM module from bytecode bytes.
    pub fn register_module(&self, name: &str, wasm_bytes: &[u8]) -> Result<Vec<String>> {
        let module = Arc::new(WasmModule::from_bytes(wasm_bytes)?);
        let funcs = module.exported_functions();
        self.modules.write().insert(name.to_string(), module);
        Ok(funcs)
    }

    /// Remove a registered WASM module.
    pub fn unregister_module(&self, name: &str) -> bool {
        self.modules.write().remove(name).is_some()
    }

    /// Execute a function from a registered module.
    pub fn call_module_func(
        &self,
        module_name: &str,
        func_name: &str,
        args: &[Value],
    ) -> Result<Value> {
        let module = {
            let guard = self.modules.read();
            guard.get(module_name).cloned()
        };
        let module = match module {
            Some(m) => m,
            None => {
                let possible_paths = [
                    format!("plugins/wasm/{}.wasm", module_name),
                    format!("etc/plugins/wasm/{}.wasm", module_name),
                    format!("data/{}.wasm", module_name),
                    format!("/kuiper/data/{}.wasm", module_name),
                    format!("{}.wasm", module_name),
                ];
                let mut loaded = None;
                for p in &possible_paths {
                    if let Ok(bytes) = std::fs::read(p) {
                        let _ = self.register_module(module_name, &bytes);
                        loaded = self.modules.read().get(module_name).cloned();
                        break;
                    }
                }
                loaded.ok_or_else(|| {
                    anyhow::anyhow!("WASM module '{}' is not registered", module_name)
                })?
            }
        };

        module.call_function(func_name, args)
    }

    /// Returns a list of all registered WASM module names.
    pub fn list_modules(&self) -> Vec<String> {
        let mut list: Vec<String> = self.modules.read().keys().cloned().collect();
        list.sort();
        list
    }

    /// Register a WASM module and automatically bind all its exported functions
    /// as global UDFs in PluginManager.
    pub fn register_module_as_udfs(
        &self,
        module_name: &str,
        wasm_bytes: &[u8],
    ) -> Result<Vec<String>> {
        let funcs = self.register_module(module_name, wasm_bytes)?;
        let module = self
            .modules
            .read()
            .get(module_name)
            .cloned()
            .expect("just registered");
        let udf_reg = crate::plugin::get_global_udf_registry();
        for func in &funcs {
            let mod_ref = module.clone();
            let fn_name = func.clone();
            let handler = Arc::new(move |args: &[Value]| {
                mod_ref.call_function(&fn_name, args).unwrap_or(Value::Null)
            });
            udf_reg.register_udf(func, handler.clone());
            udf_reg.register_udf(&format!("{}:{}", module_name, func), handler);
        }
        Ok(funcs)
    }
}

static GLOBAL_WASM_REGISTRY: std::sync::LazyLock<WasmPluginRegistry> =
    std::sync::LazyLock::new(WasmPluginRegistry::new);

/// Process-wide WASM module registry.
pub fn get_global_wasm_registry() -> &'static WasmPluginRegistry {
    &GLOBAL_WASM_REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_execution() {
        // Handcrafted minimal valid WASM bytecode for add(i32, i32) -> i32:
        let wasm_binary = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // Magic + version
            0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, // Type: (i32, i32) -> i32
            0x03, 0x02, 0x01, 0x00, // Function index 0
            0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00, // Export "add"
            0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a,
            0x0b, // Code: body_size=7, locals=0, get 0, get 1, add, end
        ];

        let module = WasmModule::from_bytes(&wasm_binary).expect("WASM compilation");
        let exports = module.exported_functions();
        assert!(exports.contains(&"add".to_string()));

        let res = module
            .call_function("add", &[serde_json::json!(15), serde_json::json!(27)])
            .expect("WASM call");
        assert_eq!(res, serde_json::json!(42));

        let registry = WasmPluginRegistry::new();
        registry
            .register_module("math", &wasm_binary)
            .expect("register module");
        let call_res = registry
            .call_module_func(
                "math",
                "add",
                &[serde_json::json!(100), serde_json::json!(200)],
            )
            .expect("registry call");
        assert_eq!(call_res, serde_json::json!(300));
    }

    #[test]
    fn test_wasm_infinite_loop_fuel_exhaustion() {
        // Minimal WASM module exporting `spin() -> i32` containing `loop { br 0 }`
        let wasm_spin = vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // Magic + version
            0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f, // Type 0: () -> i32
            0x03, 0x02, 0x01, 0x00, // Function 0 uses Type 0
            0x07, 0x08, 0x01, 0x04, 0x73, 0x70, 0x69, 0x6e, 0x00, 0x00, // Export "spin"
            0x0a, 0x09, 0x01, 0x07, 0x00, 0x03, 0x7f, 0x0c, 0x00, 0x0b,
            0x0b, // Code: loop (result i32) { br 0 } end
        ];

        let module = WasmModule::from_bytes(&wasm_spin).expect("WASM spin module compilation");
        let err = module
            .call_function("spin", &[])
            .expect_err("Infinite loop must trap on fuel exhaustion");
        assert!(
            err.to_string().contains("WASM execution trapped"),
            "Unexpected error message: {}",
            err
        );
    }
}
