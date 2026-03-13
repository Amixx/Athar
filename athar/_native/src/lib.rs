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

#[pymodule]
fn _core(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(native_entity_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(native_wl_round, module)?)?;
    module.add("__doc__", "Native accelerators for Athar hot loops.")?;
    let _ = py;
    Ok(())
}
