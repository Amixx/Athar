use std::collections::{HashMap, HashSet};
use std::time::Instant;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use xxhash_rust::xxh3::Xxh3;

pub fn native_wl_round(
    colors: &Bound<'_, PyAny>,
    adjacency: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let (step_ids, color_lookup) = extract_colors(colors)?;
    let adjacency_lookup = extract_adjacency(adjacency)?;
    let next_colors = wl_round(&step_ids, &color_lookup, &adjacency_lookup);

    let py_dict = PyDict::new(colors.py());
    for step_id in step_ids {
        if let Some(color) = next_colors.get(&step_id) {
            py_dict.set_item(step_id, color)?;
        }
    }
    Ok(py_dict.into_any().unbind())
}

pub fn native_wl_refine(
    colors: &Bound<'_, PyAny>,
    adjacency: &Bound<'_, PyAny>,
    max_rounds: usize,
) -> PyResult<PyObject> {
    let py = colors.py();
    let (step_ids, mut color_lookup) = extract_colors(colors)?;
    let adjacency_lookup = extract_adjacency(adjacency)?;

    let rounds_list = PyList::empty(py);
    let mut previous_class_count = color_lookup.values().collect::<HashSet<_>>().len();
    let mut stagnant_rounds = 0usize;
    let mut stop_reason = "max_rounds".to_string();

    for round_idx in 1..=max_rounds {
        let round_started = Instant::now();
        let next_colors = wl_round(&step_ids, &color_lookup, &adjacency_lookup);

        let mut changed = 0usize;
        for step_id in &step_ids {
            if next_colors.get(step_id) != color_lookup.get(step_id) {
                changed += 1;
            }
        }

        let class_count = next_colors.values().collect::<HashSet<_>>().len();
        if class_count == previous_class_count {
            stagnant_rounds += 1;
        } else {
            stagnant_rounds = 0;
        }

        let round_stat = PyDict::new(py);
        round_stat.set_item("round", round_idx)?;
        round_stat.set_item("changed", changed)?;
        round_stat.set_item("class_count", class_count)?;
        round_stat.set_item(
            "elapsed_ms",
            ((round_started.elapsed().as_secs_f64() * 1000.0) * 1000.0).round() / 1000.0,
        )?;
        rounds_list.append(round_stat)?;

        color_lookup = next_colors;
        previous_class_count = class_count;
        if changed == 0 {
            stop_reason = "no_color_change".to_string();
            break;
        }
        if stagnant_rounds >= 2 {
            stop_reason = "partition_stable".to_string();
            break;
        }
    }

    let color_dict = PyDict::new(py);
    for step_id in step_ids {
        if let Some(color) = color_lookup.get(&step_id) {
            color_dict.set_item(step_id, color)?;
        }
    }

    let result = PyDict::new(py);
    result.set_item("colors", color_dict)?;
    result.set_item("rounds", rounds_list)?;
    result.set_item("stop_reason", stop_reason)?;
    Ok(result.into_any().unbind())
}

fn extract_colors(colors: &Bound<'_, PyAny>) -> PyResult<(Vec<i64>, HashMap<i64, String>)> {
    let colors_dict = colors
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("colors must be a dict"))?;
    let mut color_lookup: HashMap<i64, String> = HashMap::with_capacity(colors_dict.len());
    let mut step_ids: Vec<i64> = Vec::with_capacity(colors_dict.len());
    for (step_id, color) in colors_dict.iter() {
        let step_id = step_id.extract::<i64>()?;
        let color = color.extract::<String>()?;
        color_lookup.insert(step_id, color);
        step_ids.push(step_id);
    }
    Ok((step_ids, color_lookup))
}

fn extract_adjacency(
    adjacency: &Bound<'_, PyAny>,
) -> PyResult<HashMap<i64, Vec<(String, String, i64)>>> {
    let adjacency_dict = adjacency
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("adjacency must be a dict"))?;
    let mut adjacency_lookup: HashMap<i64, Vec<(String, String, i64)>> =
        HashMap::with_capacity(adjacency_dict.len());
    for (step_id, edges) in adjacency_dict.iter() {
        let step_id = step_id.extract::<i64>()?;
        let edges = edges
            .downcast::<PyList>()
            .map_err(|_| PyTypeError::new_err("adjacency entries must be lists"))?;
        let mut edge_items: Vec<(String, String, i64)> = Vec::with_capacity(edges.len());
        for edge in edges.iter() {
            let (path, target_type, target_id) = edge.extract::<(String, Option<String>, i64)>()?;
            edge_items.push((path, target_type.unwrap_or_default(), target_id));
        }
        adjacency_lookup.insert(step_id, edge_items);
    }
    Ok(adjacency_lookup)
}

fn wl_round(
    step_ids: &[i64],
    color_lookup: &HashMap<i64, String>,
    adjacency_lookup: &HashMap<i64, Vec<(String, String, i64)>>,
) -> HashMap<i64, String> {
    let mut next_colors = HashMap::with_capacity(step_ids.len());
    for step_id in step_ids {
        let self_color = color_lookup
            .get(step_id)
            .expect("step id missing from color lookup");
        let next_color = hash_wl_round(
            self_color,
            adjacency_lookup.get(step_id).map(Vec::as_slice),
            color_lookup,
        );
        next_colors.insert(*step_id, next_color);
    }
    next_colors
}

fn hash_wl_round(
    self_color: &str,
    edges: Option<&[(String, String, i64)]>,
    color_lookup: &HashMap<i64, String>,
) -> String {
    let mut hasher = Xxh3::new();
    hasher.update(self_color.as_bytes());

    let Some(edges) = edges else {
        return format!("{:016x}", hasher.digest());
    };
    if edges.is_empty() {
        return format!("{:016x}", hasher.digest());
    }

    let mut neighbor_items: Vec<(String, String, String)> = Vec::with_capacity(edges.len());
    for (path, target_type, target_id) in edges {
        let target_color = color_lookup
            .get(target_id)
            .cloned()
            .unwrap_or_else(|| "MISSING".to_string());
        neighbor_items.push((path.clone(), target_type.clone(), target_color));
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
    format!("{:016x}", hasher.digest())
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
