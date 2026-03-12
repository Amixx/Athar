use pyo3::prelude::*;

pub fn native_wl_round(
    colors: &Bound<'_, PyAny>,
    _adjacency: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    // Placeholder scaffold. The first native milestone is fingerprinting;
    // WL round acceleration will replace this passthrough implementation.
    Ok(colors.clone().unbind())
}
