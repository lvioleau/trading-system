//! PowerBot message models, generated from `api/openapi.yaml` by
//! `scripts/regen-models.sh`. Do not edit `src/models/` by hand.
//!
//! The generated files live under a `models` module (not the crate root)
//! because the generator emits `use crate::models;` paths.

// Generated code isn't lint-clean (unused `use crate::models;`, derivable
// Default impls, empty doc comments, ...) and fixing it by hand would not
// survive a regen, so clippy is silenced for the module wholesale.
#[allow(unused_imports, clippy::all)]
pub mod models;
pub use models::*;
