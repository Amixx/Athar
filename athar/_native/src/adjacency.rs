use std::collections::{HashMap, HashSet};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

type EdgeTuple = (String, Option<String>, i64);

struct RefRecord {
    path: String,
    target_type: Option<String>,
    target_step: i64,
}

struct EntityRecord {
    step_id: i64,
    entity_type: Option<String>,
    refs: Vec<RefRecord>,
}

pub fn native_build_adjacency_maps(
    entities: &Bound<'_, PyAny>,
) -> PyResult<(PyObject, PyObject)> {
    let py = entities.py();
    let entity_records = extract_entity_records(entities)?;
    let step_ids: Vec<i64> = entity_records.iter().map(|record| record.step_id).collect();
    let step_set: HashSet<i64> = step_ids.iter().copied().collect();
    let mut adjacency_lookup: HashMap<i64, Vec<EdgeTuple>> = HashMap::with_capacity(step_ids.len());
    let mut reverse_lookup: HashMap<i64, Vec<EdgeTuple>> = HashMap::with_capacity(step_ids.len());

    for step_id in &step_ids {
        adjacency_lookup.insert(*step_id, Vec::new());
        reverse_lookup.insert(*step_id, Vec::new());
    }

    for entity in &entity_records {
        let adjacency_edges = adjacency_lookup
            .get_mut(&entity.step_id)
            .expect("missing preinitialized adjacency entry");
        for ref_record in &entity.refs {
            if !step_set.contains(&ref_record.target_step) {
                continue;
            }
            adjacency_edges.push((
                ref_record.path.clone(),
                ref_record.target_type.clone(),
                ref_record.target_step,
            ));
            reverse_lookup
                .get_mut(&ref_record.target_step)
                .expect("missing preinitialized reverse entry")
                .push((
                    ref_record.path.clone(),
                    entity.entity_type.clone(),
                    entity.step_id,
                ));
        }
        sort_edges(adjacency_edges);
    }

    for step_id in &step_ids {
        if let Some(reverse_edges) = reverse_lookup.get_mut(step_id) {
            sort_edges(reverse_edges);
        }
    }

    let adjacency_dict = PyDict::new(py);
    let reverse_dict = PyDict::new(py);
    for step_id in step_ids {
        let adjacency_list = PyList::empty(py);
        for (path, target_type, target_step) in adjacency_lookup
            .remove(&step_id)
            .unwrap_or_default()
        {
            adjacency_list.append((path, target_type, target_step))?;
        }
        adjacency_dict.set_item(step_id, adjacency_list)?;

        let reverse_list = PyList::empty(py);
        for (path, source_type, source_step) in reverse_lookup
            .remove(&step_id)
            .unwrap_or_default()
        {
            reverse_list.append((path, source_type, source_step))?;
        }
        reverse_dict.set_item(step_id, reverse_list)?;
    }

    Ok((
        adjacency_dict.into_any().unbind(),
        reverse_dict.into_any().unbind(),
    ))
}

fn extract_entity_records(entities: &Bound<'_, PyAny>) -> PyResult<Vec<EntityRecord>> {
    let entities_dict = entities
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("entities must be a dict"))?;
    let mut records = Vec::with_capacity(entities_dict.len());
    for (step_id, entity) in entities_dict.iter() {
        let step_id = step_id.extract::<i64>()?;
        let entity = entity
            .downcast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("entity records must be dicts"))?;
        let entity_type = extract_optional_string(entity.get_item("entity_type")?)?;
        let refs = match entity.get_item("refs")? {
            Some(items) => extract_refs(&items)?,
            None => Vec::new(),
        };
        records.push(EntityRecord {
            step_id,
            entity_type,
            refs,
        });
    }
    Ok(records)
}

fn extract_refs(refs: &Bound<'_, PyAny>) -> PyResult<Vec<RefRecord>> {
    let refs_list = refs
        .downcast::<PyList>()
        .map_err(|_| PyTypeError::new_err("entity refs must be lists"))?;
    let mut out = Vec::with_capacity(refs_list.len());
    for item in refs_list.iter() {
        let ref_dict = item
            .downcast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("entity refs must contain dict items"))?;
        let target_step = match ref_dict.get_item("target")? {
            Some(value) if !value.is_none() => value.extract::<i64>()?,
            _ => continue,
        };
        let path = match ref_dict.get_item("path")? {
            Some(value) if !value.is_none() => value.extract::<String>()?,
            _ => String::new(),
        };
        let target_type = extract_optional_string(ref_dict.get_item("target_type")?)?;
        out.push(RefRecord {
            path,
            target_type,
            target_step,
        });
    }
    Ok(out)
}

fn extract_optional_string(value: Option<Bound<'_, PyAny>>) -> PyResult<Option<String>> {
    match value {
        Some(value) if !value.is_none() => Ok(Some(value.extract::<String>()?)),
        _ => Ok(None),
    }
}

fn sort_edges(edges: &mut Vec<EdgeTuple>) {
    edges.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| {
                left.1
                    .as_deref()
                    .unwrap_or("")
                    .cmp(right.1.as_deref().unwrap_or(""))
            })
            .then_with(|| left.2.cmp(&right.2))
    });
}
