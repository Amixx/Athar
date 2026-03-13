use std::collections::HashMap;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use xxhash_rust::xxh3::Xxh3;

pub fn native_wl_round(
    colors: &Bound<'_, PyAny>,
    adjacency: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let colors_dict = colors
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("colors must be a dict"))?;
    let adjacency_dict = adjacency
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("adjacency must be a dict"))?;

    let mut color_lookup: HashMap<i64, String> = HashMap::with_capacity(colors_dict.len());
    let mut step_ids: Vec<i64> = Vec::with_capacity(colors_dict.len());
    for (step_id, color) in colors_dict.iter() {
        let step_id = step_id.extract::<i64>()?;
        let color = color.extract::<String>()?;
        color_lookup.insert(step_id, color);
        step_ids.push(step_id);
    }

    let next_colors = PyDict::new(colors.py());
    for step_id in step_ids {
        let self_color = color_lookup
            .get(&step_id)
            .ok_or_else(|| PyTypeError::new_err("missing self color"))?;
        let next_color = hash_wl_round(
            self_color,
            adjacency_dict.get_item(step_id)?.as_ref(),
            &color_lookup,
        )?;
        next_colors.set_item(step_id, next_color)?;
    }
    Ok(next_colors.into_any().unbind())
}

fn hash_wl_round(
    self_color: &str,
    edges: Option<&Bound<'_, PyAny>>,
    color_lookup: &HashMap<i64, String>,
) -> PyResult<String> {
    let mut hasher = Xxh3::new();
    hasher.update(self_color.as_bytes());

    let Some(item) = edges else {
        return Ok(format!("{:016x}", hasher.digest()));
    };
    if item.is_none() {
        return Ok(format!("{:016x}", hasher.digest()));
    }

    let list = item
        .downcast::<PyList>()
        .map_err(|_| PyTypeError::new_err("adjacency entries must be lists"))?;
    if list.is_empty() {
        return Ok(format!("{:016x}", hasher.digest()));
    }

    let mut neighbor_items: Vec<(String, String, String)> = Vec::with_capacity(list.len());
    for edge in list.iter() {
        let (path, target_type, target_id) = edge.extract::<(String, Option<String>, i64)>()?;
        let target_color = color_lookup
            .get(&target_id)
            .cloned()
            .unwrap_or_else(|| "MISSING".to_string());
        neighbor_items.push((path, target_type.unwrap_or_default(), target_color));
    }
    neighbor_items.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });

    let mut current = &neighbor_items[0];
    let mut count = 1usize;
    for item in neighbor_items.iter().skip(1) {
        if item == current {
            count += 1;
            continue;
        }
        append_neighbor_token(&mut hasher, current, count);
        current = item;
        count = 1;
    }
    append_neighbor_token(&mut hasher, current, count);
    Ok(format!("{:016x}", hasher.digest()))
}

fn append_neighbor_token(hasher: &mut Xxh3, item: &(String, String, String), count: usize) {
    hasher.update(b"\x1e");
    hasher.update(item.0.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(item.1.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(item.2.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(count.to_string().as_bytes());
}
