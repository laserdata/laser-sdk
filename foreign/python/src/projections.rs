use crate::async_bridge::{Awaitable, awaitable};
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let projection: Projection = py_to_de(projection)?;
        awaitable(py, async move {
            laser
                .projections()
                .register(projection)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop a projection by id. The managed host stops applying it. Existing rows stay.
    fn drop<'py>(&self, py: Python<'py>, id: String) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let mut projection: Projection = py_to_de(projection)?;
        // A graph projection is always `kind = Graph`, so the dict need not carry
        // the wire code: set it here from the entity schema the caller passed.
        projection.kind = ProjectionKind::Graph;
        awaitable(py, async move {
            laser
                .projections()
                .register_graph(projection)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop the graph projection registered under `id`. Materialized nodes and
    /// edges are left untouched.
    fn drop_graph<'py>(&self, py: Python<'py>, id: String) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            laser.projections().drop_graph(id).await.map_err(to_pyerr)
        })
    }

    /// Each binding's status dict for projection `id`, or None when no
    /// projection has it.
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[typing.Optional[list[dict[str, typing.Any]]]]",
        imports = ("typing",)
    ))]
    fn status<'py>(&self, py: Python<'py>, id: String) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            let statuses = laser.projections().status(id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &statuses))
        })
    }

    /// Read one projection's details by id, or `None` when no projection has it.
    fn get<'py>(&self, py: Python<'py>, id: String) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            let info = laser.projections().get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match info {
                Some(info) => ser_to_py(py, &info),
                None => Ok(py.None()),
            })
        })
    }

    /// One page of the projections, narrowed by a bound `topic` or any of
    /// `topics`, a `name_contains` substring, an `id_prefix`, or a `search`
    /// substring of the id or the name. No filter lists every projection.
    /// `page` is 1-based and `results` the projections per page, zero for the
    /// server's default of 10, at most 100. The dict holds `items`, `page`,
    /// `total_pages` and `total_results`.
    #[pyo3(signature = (*, topic=None, topics=None, name_contains=None, id_prefix=None, search=None, page=0, results=0))]
    #[allow(clippy::too_many_arguments)]
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[dict[str, typing.Any]]",
        imports = ("typing",)
    ))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        topic: Option<String>,
        topics: Option<Vec<String>>,
        name_contains: Option<String>,
        id_prefix: Option<String>,
        search: Option<String>,
        page: u32,
        results: u32,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            let mut request = laser.projections().list().page(page).results(results);
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let binding: ProjectionBinding = py_to_de(binding)?;
        awaitable(py, async move {
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        let source: SourceSelector = py_to_de(source)?;
        awaitable(py, async move {
            laser
                .bindings()
                .remove(source, projection_ref)
                .await
                .map_err(to_pyerr)
        })
    }
}

/// The writer-schema registry (Avro, Protobuf), built with `laser.schemas()`:
/// the default stream's registry, or the deployment-wide one without a default
/// stream. Ids are unique within one registry, and a registry holds at most
/// 1,024 schemas, tombstones included.
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
    ) -> PyResult<Awaitable<'py, u32>> {
        let laser = self.laser.clone();
        let source = py_to_de(source)?;
        awaitable(py, async move {
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
    fn drop<'py>(&self, py: Python<'py>, id: u32) -> PyResult<Awaitable<'py, ()>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            laser.schemas().drop(id).await.map_err(to_pyerr)
        })
    }

    /// Read the writer schema at `id` (active or tombstoned), or `None` when the
    /// id is free.
    fn get<'py>(&self, py: Python<'py>, id: u32) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            let info = laser.schemas().get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match info {
                Some(info) => ser_to_py(py, &info),
                None => Ok(py.None()),
            })
        })
    }

    /// One page of writer schemas, active and tombstoned, in id order: a dict
    /// with `items`, `page`, `total_pages` and `total_results`. `page` 0 reads
    /// the first page and `results` 0 leaves the page size to the server.
    #[pyo3(signature = (*, name_contains=None, page=0, results=0))]
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[dict[str, typing.Any]]",
        imports = ("typing",)
    ))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        name_contains: Option<String>,
        page: u32,
        results: u32,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        awaitable(py, async move {
            let mut request = laser.schemas().list().page(page).results(results);
            if let Some(substring) = name_contains {
                request = request.name_contains(substring);
            }
            let page = request.fetch().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &page))
        })
    }
}
