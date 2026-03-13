use pyo3::prelude::*;

mod fingerprint;
mod wl;

#[pyfunction]
fn native_entity_fingerprint(entity: &Bound<'_, PyAny>) -> PyResult<String> {
    fingerprint::native_entity_fingerprint(entity)
}

#[pyfunction]
fn native_wl_round(colors: &Bound<'_, PyAny>, adjacency: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    wl::native_wl_round(colors, adjacency)
}

#[pyfunction]
fn native_wl_refine(
    colors: &Bound<'_, PyAny>,
    adjacency: &Bound<'_, PyAny>,
    max_rounds: usize,
) -> PyResult<PyObject> {
    wl::native_wl_refine(colors, adjacency, max_rounds)
}

#[pymodule]
fn _core(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(native_entity_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(native_wl_round, module)?)?;
    module.add_function(wrap_pyfunction!(native_wl_refine, module)?)?;
    module.add("__doc__", "Native accelerators for Athar hot loops.")?;
    let _ = py;
    Ok(())
}
