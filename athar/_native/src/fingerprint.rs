use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use xxhash_rust::xxh3::Xxh3;

pub fn native_entity_fingerprint(entity: &Bound<'_, PyAny>) -> PyResult<String> {
    let entity_dict = entity
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("entity must be a dict"))?;

    let mut hasher = Xxh3::new();
    hash_token(&mut hasher, "entity_type");
    hash_scalar_any(&mut hasher, entity_dict.get_item("entity_type")?.as_ref())?;

    hash_token(&mut hasher, "attributes");
    if let Some(attributes) = entity_dict.get_item("attributes")? {
        hash_value_stripping_refs(&mut hasher, Some(&attributes))?;
    } else {
        hasher.update(b"{");
        hasher.update(b"}");
    }

    hash_token(&mut hasher, "edges");
    hash_edge_multiset(&mut hasher, entity_dict.get_item("refs")?.as_ref())?;

    Ok(format!("{:032x}", hasher.digest128()))
}

fn hash_token(hasher: &mut Xxh3, token: &str) {
    hasher.update(b"#");
    hash_scalar_str(hasher, token);
}

fn hash_scalar_str(hasher: &mut Xxh3, value: &str) {
    let encoded = value.as_bytes();
    hasher.update(b"S");
    hasher.update(encoded.len().to_string().as_bytes());
    hasher.update(b":");
    hasher.update(encoded);
}

fn hash_scalar_any(hasher: &mut Xxh3, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    match value {
        None => {
            hasher.update(b"N");
            Ok(())
        }
        Some(item) if item.is_none() => {
            hasher.update(b"N");
            Ok(())
        }
        Some(item) => {
            if item.is_instance_of::<PyBool>() {
                let value = item.extract::<bool>()?;
                hasher.update(if value { b"B1" } else { b"B0" });
                return Ok(());
            }
            if item.is_instance_of::<PyInt>() && !item.is_instance_of::<PyBool>() {
                let value = item.extract::<i64>()?;
                hasher.update(b"I");
                hasher.update(value.to_string().as_bytes());
                hasher.update(b";");
                return Ok(());
            }
            if item.is_instance_of::<PyFloat>() {
                let value = item.repr()?;
                let value = value.to_str()?;
                hasher.update(b"F");
                hasher.update(value.as_bytes());
                hasher.update(b";");
                return Ok(());
            }
            let text_obj = if item.is_instance_of::<PyString>() {
                item.downcast::<PyString>()?.clone()
            } else {
                item.str()?
            };
            let text = text_obj.to_str()?;
            hash_scalar_str(hasher, text);
            Ok(())
        }
    }
}

fn hash_value_stripping_refs(hasher: &mut Xxh3, value: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let Some(item) = value else {
        hasher.update(b"N");
        return Ok(());
    };
    if item.is_none() {
        hasher.update(b"N");
        return Ok(());
    }

    if let Ok(dict) = item.downcast::<PyDict>() {
        if let Some(kind) = dict.get_item("kind")? {
            if kind.extract::<&str>()? == "ref" {
                hasher.update(b"R");
                return Ok(());
            }
        }
        let mut keys: Vec<String> = dict
            .keys()
            .iter()
            .map(|key| key.extract::<String>())
            .collect::<PyResult<Vec<_>>>()?;
        keys.sort();
        hasher.update(b"{");
        for key in keys {
            hasher.update(b"K");
            hash_scalar_str(hasher, &key);
            hasher.update(b"V");
            hash_value_stripping_refs(hasher, dict.get_item(&key)?.as_ref())?;
            hasher.update(b";");
        }
        hasher.update(b"}");
        return Ok(());
    }

    if let Ok(list) = item.downcast::<PyList>() {
        hasher.update(b"[");
        for list_item in list.iter() {
            hash_value_stripping_refs(hasher, Some(&list_item))?;
            hasher.update(b",");
        }
        hasher.update(b"]");
        return Ok(());
    }

    hash_scalar_any(hasher, Some(item))
}

fn hash_edge_multiset(hasher: &mut Xxh3, refs: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let Some(item) = refs else {
        hasher.update(b"E{}");
        return Ok(());
    };
    let list = item
        .downcast::<PyList>()
        .map_err(|_| PyTypeError::new_err("refs must be a list"))?;

    let mut edges: Vec<(String, Option<String>)> = Vec::with_capacity(list.len());
    for ref_item in list.iter() {
        let dict = ref_item
            .downcast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("ref entries must be dicts"))?;
        let path = dict
            .get_item("path")?
            .map(|item| item.extract::<String>())
            .transpose()?
            .unwrap_or_default();
        let target_type = match dict.get_item("target_type")? {
            None => None,
            Some(item) if item.is_none() => None,
            Some(item) => Some(item.extract::<String>()?),
        };
        edges.push((path, target_type));
    }
    edges.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            left.1
                .as_deref()
                .unwrap_or("")
                .cmp(right.1.as_deref().unwrap_or(""))
        })
    });

    hasher.update(b"E{");
    let mut index = 0usize;
    while index < edges.len() {
        let current = &edges[index];
        let mut count = 1usize;
        while index + count < edges.len() && edges[index + count] == *current {
            count += 1;
        }
        hasher.update(b"P");
        hash_scalar_str(hasher, &current.0);
        hasher.update(b"T");
        match &current.1 {
            Some(value) => hash_scalar_str(hasher, value),
            None => {
                hasher.update(b"N");
            }
        }
        hasher.update(b"C");
        hasher.update(b"I");
        hasher.update(count.to_string().as_bytes());
        hasher.update(b";");
        hasher.update(b";");
        index += count;
    }
    hasher.update(b"}");
    Ok(())
}
