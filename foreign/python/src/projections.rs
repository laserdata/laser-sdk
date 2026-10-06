use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{py_to_de, ser_to_py};
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use laser_sdk::query::{Projection, ProjectionBinding, ProjectionKind, SourceSelector};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The projection registry: `register`, `register_graph`, `drop`,
    /// `drop_graph`, `get`, and the filterable `list` browse.
    fn projections(&self) -> PyProjections {
        PyProjections {
            laser: self.inner.clone(),
        }
    }

    /// The binding surface: `apply` routes a (stream, topic) source into
    /// registered projections, `remove` stops it.
    fn bindings(&self) -> PyBindings {
        PyBindings {
            laser: self.inner.clone(),
        }
    }

    /// The writer-schema registry (Avro, Protobuf): `register`, `drop`,
    /// `get`, and `list`.
    fn schemas(&self) -> PySchemas {
        PySchemas {
            laser: self.inner.clone(),
        }
    }
}

/// The projection registry, built with `laser.projections()`. Writes publish
/// control commands applied asynchronously by the managed host (poll `get(id)`
/// to observe the apply). Reads are registry browses.
#[gen_stub_pyclass]
#[pyclass(name = "Projections", frozen)]
pub struct PyProjections {
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyProjections {
    /// Register a projection from a dict matching the projection schema. A
    /// graph projection raises `InvalidError`: register it with `register_graph`.
    fn register<'py>(
        &self,
        py: Python<'py>,
        projection: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let projection: Projection = py_to_de(projection)?;
        future_into_py(py, async move {
            laser
                .projections()
                .register(projection)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop a projection by id. The managed host stops applying it. Existing rows stay.
    fn drop<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser.projections().drop(id).await.map_err(to_pyerr)
        })
    }

    /// Register a graph projection from a dict carrying an `entity_schema`. It
    /// records the named knowledge graph and its node and edge extraction plan.
    /// Graph data is written with `graph(name).upsert(..)`, or by the projector
    /// when the projection is bound to a source topic.
    fn register_graph<'py>(
        &self,
        py: Python<'py>,
        projection: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut projection: Projection = py_to_de(projection)?;
        // A graph projection is always `kind = Graph`, so the dict need not carry
        // the wire code: set it here from the entity schema the caller passed.
        projection.kind = ProjectionKind::Graph;
        future_into_py(py, async move {
            laser
                .projections()
                .register_graph(projection)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop the graph projection registered under `id`. Materialized nodes and
    /// edges are left untouched.
    fn drop_graph<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser.projections().drop_graph(id).await.map_err(to_pyerr)
        })
    }

    /// Read one projection's details by id, or `None` when no projection has it.
    fn get<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let info = laser.projections().get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match info {
                Some(info) => ser_to_py(py, &info),
                None => Ok(py.None()),
            })
        })
    }

    /// List projections, narrowed by a bound `topic` or any of `topics`, a
    /// `name_contains` substring, an `id_prefix`, or a `search` substring of
    /// the id or the name. No filter lists every projection.
    #[pyo3(signature = (*, topic=None, topics=None, name_contains=None, id_prefix=None, search=None))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        topic: Option<String>,
        topics: Option<Vec<String>>,
        name_contains: Option<String>,
        id_prefix: Option<String>,
        search: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let mut request = laser.projections().list();
            if let Some(topic) = topic {
                request = request.for_topic(topic);
            }
            if let Some(topics) = topics {
                request = request.for_topics(topics);
            }
            if let Some(name_contains) = name_contains {
                request = request.name_contains(name_contains);
            }
            if let Some(id_prefix) = id_prefix {
                request = request.id_prefix(id_prefix);
            }
            if let Some(search) = search {
                request = request.search(search);
            }
            let list = request.fetch().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &list))
        })
    }
}

/// The binding surface, built with `laser.bindings()`. Writes publish control
/// commands applied asynchronously by the managed host. Bindings are browsed
/// through the projections they route to.
#[gen_stub_pyclass]
#[pyclass(name = "Bindings", frozen)]
pub struct PyBindings {
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBindings {
    /// Apply a projection binding dict, routing a (stream, topic) source into
    /// registered projections. Register the referenced projection first.
    fn apply<'py>(
        &self,
        py: Python<'py>,
        binding: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let binding: ProjectionBinding = py_to_de(binding)?;
        future_into_py(py, async move {
            laser.bindings().apply(binding).await.map_err(to_pyerr)
        })
    }

    /// Remove the binding for `source` (a `{"stream", "topic"}` dict). Set
    /// `projection_ref` to stop routing into one projection only. Rows already
    /// written stay.
    #[pyo3(signature = (source, projection_ref=None))]
    fn remove<'py>(
        &self,
        py: Python<'py>,
        source: &Bound<'_, PyAny>,
        projection_ref: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let source: SourceSelector = py_to_de(source)?;
        future_into_py(py, async move {
            laser
                .bindings()
                .remove(source, projection_ref)
                .await
                .map_err(to_pyerr)
        })
    }
}

/// The writer-schema registry (Avro, Protobuf), built with `laser.schemas()`.
#[gen_stub_pyclass]
#[pyclass(name = "Schemas", frozen)]
pub struct PySchemas {
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySchemas {
    /// Register a writer schema from a source dict and await the
    /// managed-allocated id. `name` and `version` are stored metadata, never
    /// dispatched on.
    #[pyo3(signature = (source, *, name=None, version=None))]
    fn register<'py>(
        &self,
        py: Python<'py>,
        source: &Bound<'_, PyAny>,
        name: Option<String>,
        version: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let source = py_to_de(source)?;
        future_into_py(py, async move {
            let schemas = laser.schemas();
            let mut request = schemas.register(source);
            if let Some(name) = name {
                request = request.name(name);
            }
            if let Some(version) = version {
                request = request.version(version);
            }
            request.send().await.map_err(to_pyerr)
        })
    }

    /// Drop the writer schema at `id`. A tombstone: records stamped with it keep
    /// decoding and the id stays reserved.
    fn drop<'py>(&self, py: Python<'py>, id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser.schemas().drop(id).await.map_err(to_pyerr)
        })
    }

    /// Read the writer schema at `id` (active or tombstoned), or `None` when the
    /// id is free.
    fn get<'py>(&self, py: Python<'py>, id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let info = laser.schemas().get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match info {
                Some(info) => ser_to_py(py, &info),
                None => Ok(py.None()),
            })
        })
    }

    /// List every known writer schema, active and tombstoned.
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let list = laser.schemas().list().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &list))
        })
    }
}
