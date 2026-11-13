//! The Python shim: adapts the cradle's [`Strategy`] trait to a Python strategy module — the
//! seam where the strategy proper stops being Rust. The module contract: it must define
//! `strategy_create(params)` (returning the state object carried between ticks; `None` refuses
//! — a fatal boot error) and `strategy_targets(state, input)` (one evaluation: input dict →
//! iterable of target dicts). `py/strategy.py` documents the layout — the module the real
//! strategy repo delivers speaks exactly that contract — and is the reference implementation
//! (the mock oscillator).
//!
//! **Marshaling discipline.** Rows are the union of every key across the three views, sorted by
//! (market_type, contract_id) — a canonical order, so a replayed input is identical regardless
//! of `HashMap` iteration order. Everything crossing the boundary is integers (kW, minor
//! currency units per MWh); market types are 0 (local) and 1 (XBID). A Python exception at tick
//! time is logged (with traceback) and yields no targets — the cradle's safe-by-absence stance
//! — never a crash.

use std::ffi::CString;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tracing::{error, info, warn};

use crate::driver::{ContractId, Level, MarketType, Strategy, StrategyInput};

// The contract's market-type values — the wire between the cradle's domain enum and the
// module's rows and targets.
const PY_MARKET_LOCAL: i64 = 0;
const PY_MARKET_XBID: i64 = 1;

/// The Python strategy behind the [`Strategy`] seam: owns the state object `strategy_create`
/// returned and the module's `strategy_targets`, translating at every call. The interpreter is
/// embedded (pyo3 `auto-initialize`) and shared process-wide; calls take the GIL for exactly the
/// duration of one evaluation — serialized anyway, per the cradle's single loop.
#[derive(Debug)]
pub struct PyStrategy {
    /// The module's `strategy_targets` function.
    targets_fn: Py<PyAny>,
    /// The state `strategy_create` returned — opaque to the cradle, owned by the module.
    state: Py<PyAny>,
}

impl PyStrategy {
    /// Load the module from `path` and create the strategy's state with the cradle's tuning,
    /// passed to `strategy_create` as a params dict. Any load or create failure is a fatal
    /// boot error.
    pub fn new(
        path: &str,
        amplitude_kw: i64,
        max_contracts: usize,
        min_time_to_delivery_ms: i64,
    ) -> Result<Self> {
        let source = fs::read_to_string(path)
            .with_context(|| format!("reading the python strategy from {path}"))?;
        Self::from_source(
            &source,
            path,
            amplitude_kw,
            max_contracts,
            min_time_to_delivery_ms,
        )
        .with_context(|| format!("initializing the python strategy from {path}"))
    }

    /// Create the strategy from module source (`filename` labels tracebacks) — the seam
    /// [`new`](Self::new) wraps, and what tests drive with inline modules.
    pub fn from_source(
        source: &str,
        filename: &str,
        amplitude_kw: i64,
        max_contracts: usize,
        min_time_to_delivery_ms: i64,
    ) -> Result<Self> {
        // `from_code` registers the module in `sys.modules` under this name and, on a repeat
        // name, re-executes into the *existing* module's dict — leaking attributes from one
        // load into the next. A per-load name keeps every load hermetic.
        static MODULE_SEQ: AtomicUsize = AtomicUsize::new(0);
        let module_name = format!("strategy_{}", MODULE_SEQ.fetch_add(1, Ordering::Relaxed));
        Python::attach(|py| {
            let init = || -> PyResult<(Py<PyAny>, Py<PyAny>)> {
                let module = PyModule::from_code(
                    py,
                    CString::new(source)?.as_c_str(),
                    CString::new(filename)?.as_c_str(),
                    CString::new(module_name.as_str())?.as_c_str(),
                )?;
                let targets_fn = module.getattr("strategy_targets")?.unbind();
                let params = PyDict::new(py);
                params.set_item("amplitude_kw", amplitude_kw)?;
                params.set_item("max_contracts", max_contracts)?;
                params.set_item("min_time_to_delivery_ms", min_time_to_delivery_ms)?;
                let state = module
                    .getattr("strategy_create")?
                    .call1((params,))?
                    .unbind();
                Ok((targets_fn, state))
            };
            let (targets_fn, state) = init().map_err(|err| anyhow::anyhow!(describe(py, &err)))?;
            if state.is_none(py) {
                bail!("strategy_create returned None");
            }
            Ok(Self { targets_fn, state })
        })
    }
}

impl Strategy for PyStrategy {
    fn targets(&mut self, input: &StrategyInput) -> Vec<(ContractId, i64)> {
        // The canonical row order: the union of every key the input mentions, sorted.
        let mut keys: Vec<ContractId> = input
            .contracts
            .keys()
            .chain(input.positions.keys())
            .chain(input.books.keys())
            .copied()
            .collect();
        keys.sort_unstable();
        keys.dedup();

        info!(
            now_ms = input.now_ms,
            rows = keys.len(),
            contracts = input.contracts.len(),
            positions = input.positions.len(),
            books = input.books.len(),
            "calling the strategy"
        );
        let called_at = Instant::now();
        let result = Python::attach(|py| {
            let call = || -> PyResult<Vec<(ContractId, i64)>> {
                let rows = PyList::empty(py);
                for key in &keys {
                    rows.append(to_py_row(py, *key, input)?)?;
                }
                let py_input = PyDict::new(py);
                py_input.set_item("now_ms", input.now_ms)?;
                py_input.set_item("rows", rows)?;
                let returned = self
                    .targets_fn
                    .bind(py)
                    .call1((self.state.bind(py), py_input))?;
                let mut targets = Vec::new();
                for item in returned.try_iter()? {
                    let item = item?;
                    let market_type = item.get_item("market_type")?.extract::<i64>()?;
                    let contract_id = item.get_item("contract_id")?.extract::<i64>()?;
                    let target_kw = item.get_item("target_kw")?.extract::<i64>()?;
                    match from_py_market(market_type) {
                        Some(market_type) => targets.push(((market_type, contract_id), target_kw)),
                        None => warn!(
                            market_type,
                            contract_id,
                            "strategy returned a target with an unknown market type; dropped"
                        ),
                    }
                }
                Ok(targets)
            };
            call().map_err(|err| describe(py, &err))
        });
        match result {
            Ok(targets) => {
                info!(
                    targets = targets.len(),
                    elapsed_us = called_at.elapsed().as_micros() as u64,
                    "strategy returned"
                );
                targets
            }
            Err(err) => {
                error!(error = %err, "the python strategy failed; no targets this tick");
                Vec::new()
            }
        }
    }
}

/// One contract's row of the input dict — the Python rendering of `strategy_contract_row`:
/// the contract view, position, and book joined per key, absent parts as the documented zero
/// values (0 / False / empty sides).
fn to_py_row<'py>(
    py: Python<'py>,
    key: ContractId,
    input: &StrategyInput,
) -> PyResult<Bound<'py, PyDict>> {
    let view = input.contracts.get(&key);
    let book = input.books.get(&key);
    let row = PyDict::new(py);
    row.set_item("market_type", to_py_market(key.0))?;
    row.set_item("contract_id", key.1)?;
    row.set_item("delivery_start_ms", view.map_or(0, |v| v.delivery_start_ms))?;
    row.set_item("active", view.is_some_and(|v| v.active))?;
    row.set_item(
        "position_kw",
        input.positions.get(&key).copied().unwrap_or(0),
    )?;
    row.set_item("bids", to_py_levels(py, book.map_or(&[][..], |b| &b.bids))?)?;
    row.set_item("asks", to_py_levels(py, book.map_or(&[][..], |b| &b.asks))?)?;
    Ok(row)
}

/// One book side as a list of `(price, quantity)` tuples, best-first — integers verbatim.
fn to_py_levels<'py>(py: Python<'py>, levels: &[Level]) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, levels.iter().map(|level| (level.price, level.quantity)))
}

fn to_py_market(market_type: MarketType) -> i64 {
    match market_type {
        MarketType::Local => PY_MARKET_LOCAL,
        MarketType::Xbid => PY_MARKET_XBID,
    }
}

fn from_py_market(value: i64) -> Option<MarketType> {
    match value {
        PY_MARKET_LOCAL => Some(MarketType::Local),
        PY_MARKET_XBID => Some(MarketType::Xbid),
        _ => None,
    }
}

/// Render a Python error for logs and boot failures: the traceback (when one exists) followed
/// by the exception itself.
fn describe(py: Python<'_>, err: &PyErr) -> String {
    let traceback = err
        .traceback(py)
        .and_then(|tb| tb.format().ok())
        .unwrap_or_default();
    format!("{traceback}{err}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::driver::{Book, ContractView};

    fn input(
        now_ms: i64,
        contracts: Vec<(ContractId, ContractView)>,
        positions: Vec<(ContractId, i64)>,
        books: Vec<(ContractId, Book)>,
    ) -> StrategyInput {
        StrategyInput {
            now_ms,
            contracts: contracts.into_iter().collect(),
            positions: positions.into_iter().collect(),
            books: books.into_iter().collect(),
        }
    }

    fn view(delivery_start_ms: i64, active: bool) -> ContractView {
        ContractView {
            delivery_start_ms,
            active,
        }
    }

    fn from_source(source: &str) -> Result<PyStrategy> {
        PyStrategy::from_source(source, "test-strategy.py", 10_000, 2, HOUR_MS)
    }

    const HOUR_MS: i64 = 3_600_000;
    /// A delivery start far beyond every test timestamp — always outside the no-trade window.
    const FAR_OUT_MS: i64 = 4_000_000_000_000;

    /// The full round trip, asserted from the Python side: the params dict reaches
    /// `strategy_create`, the state object comes back on every call, and each row carries the
    /// joined views — the live contract's fields verbatim, and the documented zero values
    /// (0 / False / empty sides) for a position-only row outside the live view. A failed assert
    /// raises, which yields no targets, so the returned target doubles as the pass signal.
    #[test]
    fn rows_carry_the_joined_views_and_zero_value_absences() {
        let mut strategy = PyStrategy::from_source(
            r#"
def strategy_create(params):
    assert params == {"amplitude_kw": 7, "max_contracts": 3, "min_time_to_delivery_ms": 9}
    return "the state"

def strategy_targets(state, input):
    assert state == "the state"
    assert input["now_ms"] == 1000
    [held, traded] = input["rows"]
    assert held == {
        "market_type": 0,
        "contract_id": 999,
        "delivery_start_ms": 0,
        "active": False,
        "position_kw": -7000,
        "bids": [],
        "asks": [],
    }
    assert traded == {
        "market_type": 1,
        "contract_id": 42,
        "delivery_start_ms": 5000,
        "active": True,
        "position_kw": 1500,
        "bids": [(4100, 5000), (4000, 1000)],
        "asks": [(4300, 2000)],
    }
    return [{"market_type": 1, "contract_id": 42, "target_kw": 500}]
"#,
            "test-strategy.py",
            7,
            3,
            9,
        )
        .expect("loading the module");
        let traded = (MarketType::Xbid, 42);
        let held = (MarketType::Local, 999);
        let targets = strategy.targets(&input(
            1000,
            vec![(traded, view(5000, true))],
            vec![(held, -7_000), (traded, 1_500)],
            vec![(
                traded,
                Book {
                    bids: vec![
                        Level {
                            price: 4_100,
                            quantity: 5_000,
                        },
                        Level {
                            price: 4_000,
                            quantity: 1_000,
                        },
                    ],
                    asks: vec![Level {
                        price: 4_300,
                        quantity: 2_000,
                    }],
                },
            )],
        ));
        assert_eq!(targets, vec![(traded, 500)], "a python assert failed");
    }

    /// Rows arrive in the canonical order — the sorted union of every key across the three
    /// views.
    #[test]
    fn rows_are_the_sorted_union_of_the_three_views() {
        let mut strategy = from_source(
            r#"
def strategy_create(params):
    return {}

def strategy_targets(state, input):
    return [
        {"market_type": row["market_type"], "contract_id": row["contract_id"], "target_kw": index}
        for index, row in enumerate(input["rows"])
    ]
"#,
        )
        .expect("loading the module");
        let targets = strategy.targets(&input(
            0,
            vec![((MarketType::Xbid, 200), view(FAR_OUT_MS, true))],
            vec![((MarketType::Local, 999), -7_000)],
            vec![((MarketType::Xbid, 100), Book::default())],
        ));
        assert_eq!(
            targets,
            vec![
                ((MarketType::Local, 999), 0),
                ((MarketType::Xbid, 100), 1),
                ((MarketType::Xbid, 200), 2),
            ]
        );
    }

    /// State lives behind the object `strategy_create` returned, across ticks — the Python
    /// opaque handle the module owns.
    #[test]
    fn state_persists_between_ticks() {
        let mut strategy = from_source(
            r#"
def strategy_create(params):
    return {"calls": 0}

def strategy_targets(state, input):
    state["calls"] += 1
    return [{"market_type": 1, "contract_id": 1, "target_kw": state["calls"]}]
"#,
        )
        .expect("loading the module");
        let empty = input(0, vec![], vec![], vec![]);
        assert_eq!(strategy.targets(&empty), vec![((MarketType::Xbid, 1), 1)]);
        assert_eq!(strategy.targets(&empty), vec![((MarketType::Xbid, 1), 2)]);
    }

    /// A raising strategy yields no targets — withheld, never a crash. (The traceback goes to
    /// the log.)
    #[test]
    fn an_exception_yields_no_targets() {
        let mut strategy = from_source(
            r#"
def strategy_create(params):
    return object()

def strategy_targets(state, input):
    raise RuntimeError("boom")
"#,
        )
        .expect("loading the module");
        assert!(
            strategy
                .targets(&input(0, vec![], vec![], vec![]))
                .is_empty()
        );
    }

    /// An unknown market type drops that target and keeps the rest.
    #[test]
    fn an_unknown_market_type_is_dropped() {
        let mut strategy = from_source(
            r#"
def strategy_create(params):
    return object()

def strategy_targets(state, input):
    return [
        {"market_type": 7, "contract_id": 1, "target_kw": 100},
        {"market_type": 0, "contract_id": 2, "target_kw": 200},
    ]
"#,
        )
        .expect("loading the module");
        let targets = strategy.targets(&input(0, vec![], vec![], vec![]));
        assert_eq!(targets, vec![((MarketType::Local, 2), 200)]);
    }

    /// `strategy_create` returning None refuses to initialize — fatal at boot.
    #[test]
    fn a_none_state_is_a_fatal_boot_error() {
        let err = from_source(
            r#"
def strategy_create(params):
    return None

def strategy_targets(state, input):
    return []
"#,
        )
        .expect_err("None must refuse initialization");
        assert!(err.to_string().contains("None"), "{err}");
    }

    /// A module missing either contract function fails at boot, not at the first tick.
    #[test]
    fn a_module_missing_the_contract_is_a_fatal_boot_error() {
        let err = from_source("def strategy_create(params):\n    return object()\n")
            .expect_err("a module without strategy_targets must not load");
        assert!(err.to_string().contains("strategy_targets"), "{err}");
    }

    /// The reference module `py/strategy.py` traces its lot-quantized, phase-shifted sine,
    /// black-box through the shim: at a quarter period the unshifted contract
    /// (id * stride % 1000 == 0) peaks and the half-period-shifted one
    /// (id * stride % 1000 == 500) troughs.
    #[test]
    fn the_reference_module_traces_the_mock_sine() {
        let flat = (MarketType::Xbid, 7_000);
        let shifted = (MarketType::Xbid, 7_500);
        let amplitude = 10_000;
        let mut strategy = PyStrategy::new(
            concat!(env!("CARGO_MANIFEST_DIR"), "/py/strategy.py"),
            amplitude,
            2,
            HOUR_MS,
        )
        .expect("loading py/strategy.py");
        let targets: HashMap<ContractId, i64> = strategy
            .targets(&input(
                450_000, // a quarter of the 30-minute period
                vec![
                    (flat, view(FAR_OUT_MS, true)),
                    (shifted, view(FAR_OUT_MS, true)),
                ],
                vec![],
                vec![],
            ))
            .into_iter()
            .collect();
        assert_eq!(targets[&flat], amplitude);
        assert_eq!(targets[&shifted], -amplitude);
    }

    /// The reference module selects its own tradable set from the rows: with room for one it
    /// takes the soonest-delivering active XBID contract outside the no-trade window.
    #[test]
    fn the_reference_module_selects_its_own_tradable_set() {
        let chosen = (MarketType::Xbid, 100);
        let later = (MarketType::Xbid, 101);
        let inactive = (MarketType::Xbid, 102);
        let expiring = (MarketType::Xbid, 103);
        let local = (MarketType::Local, 104);
        let now_ms = 450_000;
        let mut strategy = PyStrategy::new(
            concat!(env!("CARGO_MANIFEST_DIR"), "/py/strategy.py"),
            10_000,
            1,
            HOUR_MS,
        )
        .expect("loading py/strategy.py");
        let targets = strategy.targets(&input(
            now_ms,
            vec![
                (chosen, view(now_ms + 2 * HOUR_MS, true)),
                (later, view(now_ms + 3 * HOUR_MS, true)),
                (inactive, view(now_ms + 2 * HOUR_MS, false)),
                (expiring, view(now_ms + HOUR_MS / 2, true)),
                (local, view(now_ms + 2 * HOUR_MS, true)),
            ],
            vec![],
            vec![],
        ));
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, chosen);
    }
}
