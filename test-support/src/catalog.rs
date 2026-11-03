use laser_wire::codes::{
    AGDX_BACKEND_HELLO_CODE, AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE,
    AGDX_GET_FILTER_BINDING_CODE, AGDX_LIST_FILTER_BINDINGS_CODE, AGDX_LIST_FILTER_REVISIONS_CODE,
    AGDX_LIST_FILTERS_CODE, AGDX_RESOLVE_FILTER_POLICY_CODE, CONTROL_OP_VERSION, FILTER_OP_VERSION,
    FORK_OP_VERSION, KV_OP_VERSION, QUERY_OP_VERSION,
};
use laser_wire::filter::{
    CatalogPosition, FilterBinding, FilterBindingPage, FilterCatalogCommand, FilterCatalogOutcome,
    FilterCatalogReply, FilterError, FilterErrorReason, FilterGroupIdentity, FilterMutation,
    FilterMutationOutcome, FilterMutationResult, FilterMutationStatus, FilterOwner, FilterPage,
    FilterPolicyRef, FilterRevisionInfo, FilterRevisionPage, FilterRevisionRef, FilterState,
    FilterSummary, GetFilterBinding, GroupFilterSpec, GroupPolicyUnbound, ListFilterBindings,
    ListFilterRevisions, ListFilters, ResolveFilterPolicy, ResolvedFilterPolicy,
};
use laser_wire::forward::ForwardedCommand;
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::hello::{BackendAnnounce, OpVersions};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

/// The group policies the stand-in holds.
#[derive(Default)]
pub struct CatalogState {
    next_filter_id: u32,
    offset: u64,
    pub filters: HashMap<u32, Vec<FilterRevisionInfo>>,
    pub owners: HashMap<FilterGroupIdentity, u32>,
    // The caller's name and the owning group of each group's own filter.
    pub owned: HashMap<u32, OwnedFilter>,
    bindings: HashMap<FilterGroupIdentity, FilterBinding>,
    generations: HashMap<FilterGroupIdentity, u64>,
    // Owners whose next lookup fails once.
    pub owner_lookup_failures: HashSet<FilterGroupIdentity>,
    // Owners whose lookups answer with every other filter instead.
    pub owner_lookup_mismatches: HashSet<FilterGroupIdentity>,
    pub owner_lookups: Vec<FilterGroupIdentity>,
    // The node and command code of every forwarded command, in arrival order.
    pub forwarded: Vec<(usize, u32)>,
}

pub struct OwnedFilter {
    pub name: String,
    owner: FilterOwner,
}

impl CatalogState {
    fn bump(&mut self, identity: FilterGroupIdentity) -> u64 {
        let generation = self.generations.entry(identity).or_default();
        *generation += 1;
        *generation
    }

    pub fn mutate(
        &mut self,
        command: FilterCatalogCommand,
    ) -> Result<FilterMutationResult, FilterError> {
        match command.mutation {
            FilterMutation::ConfigureGroup {
                group,
                policy,
                expected_identity,
            } => {
                let identity = command.identity.ok_or_else(|| {
                    FilterError::new(
                        FilterErrorReason::InvalidRequest,
                        "the streaming server stamps the group identity",
                    )
                })?;
                if expected_identity.is_some_and(|expected| expected != identity) {
                    return Err(FilterError::new(
                        FilterErrorReason::SourceChanged,
                        "the group incarnation changed before configuration",
                    ));
                }
                let (filter_id, revision, digest) = match policy {
                    GroupFilterSpec::Definition { name, filter } => {
                        let digest = filter.digest();
                        if let Some(existing) = self.bindings.get(&identity) {
                            if existing.digest == digest {
                                let existing = existing.clone();
                                if let Some(owned) = self.owned.get_mut(&existing.filter_id) {
                                    owned.name = name;
                                }
                                return Ok(FilterMutationResult::Bound(existing));
                            }
                            return Err(FilterError::new(
                                FilterErrorReason::Conflict,
                                "the group already runs another policy",
                            ));
                        }
                        let filter_id = match self.owners.get(&identity) {
                            Some(filter_id) => *filter_id,
                            None => {
                                self.next_filter_id += 1;
                                self.owners.insert(identity, self.next_filter_id);
                                self.next_filter_id
                            }
                        };
                        self.owned.insert(
                            filter_id,
                            OwnedFilter {
                                name,
                                owner: FilterOwner {
                                    identity,
                                    group: group.clone(),
                                },
                            },
                        );
                        let revisions = self.filters.entry(filter_id).or_default();
                        let revision = match revisions.iter().find(|info| info.digest == digest) {
                            Some(info) => info.revision,
                            None => {
                                let revision = revisions.len() as u32 + 1;
                                revisions.push(FilterRevisionInfo {
                                    enabled: true,
                                    revision,
                                    digest: digest.clone(),
                                    filter,
                                    created_at_micros: 1,
                                });
                                revision
                            }
                        };
                        (filter_id, revision, digest)
                    }
                    GroupFilterSpec::Revision {
                        filter_id,
                        revision,
                    } => {
                        if self.owners.get(&identity) != Some(&filter_id) {
                            return Err(FilterError::new(
                                FilterErrorReason::Conflict,
                                "not the group's own filter",
                            ));
                        }
                        let digest = self
                            .filters
                            .get(&filter_id)
                            .and_then(|revisions| {
                                revisions.iter().find(|info| info.revision == revision)
                            })
                            .map(|info| info.digest.clone())
                            .ok_or_else(|| {
                                FilterError::new(FilterErrorReason::NotFound, "no such revision")
                            })?;
                        if let Some(existing) = self.bindings.get(&identity) {
                            if existing.digest == digest {
                                return Ok(FilterMutationResult::Bound(existing.clone()));
                            }
                            return Err(FilterError::new(
                                FilterErrorReason::Conflict,
                                "the group already runs another policy",
                            ));
                        }
                        (filter_id, revision, digest)
                    }
                };
                let binding = FilterBinding {
                    group,
                    identity,
                    filter_id,
                    revision,
                    digest,
                    bound_at_micros: 1,
                    policy_generation: self.bump(identity),
                };
                self.bindings.insert(identity, binding.clone());
                Ok(FilterMutationResult::Bound(binding))
            }
            FilterMutation::Revise {
                filter_id,
                expected_revision,
                filter,
            } => {
                let revisions = self.filters.get_mut(&filter_id).ok_or_else(|| {
                    FilterError::new(FilterErrorReason::NotFound, "no such filter")
                })?;
                let latest = revisions.last().map_or(0, |info| info.revision);
                if latest != expected_revision {
                    return Err(FilterError::new(
                        FilterErrorReason::Conflict,
                        "not the latest revision",
                    ));
                }
                let digest = filter.digest();
                revisions.push(FilterRevisionInfo {
                    enabled: true,
                    revision: latest + 1,
                    digest: digest.clone(),
                    filter,
                    created_at_micros: 1,
                });
                Ok(FilterMutationResult::Revised(FilterRevisionRef {
                    filter_id,
                    revision: latest + 1,
                    digest,
                }))
            }
            FilterMutation::SetRevisionEnabled {
                filter_id,
                revision,
                enabled,
            } => {
                let info = self
                    .filters
                    .get_mut(&filter_id)
                    .and_then(|revisions| {
                        revisions.iter_mut().find(|info| info.revision == revision)
                    })
                    .ok_or_else(|| {
                        FilterError::new(FilterErrorReason::NotFound, "no such revision")
                    })?;
                info.enabled = enabled;
                Ok(FilterMutationResult::RevisionState {
                    filter_id,
                    revision,
                    enabled,
                })
            }
            FilterMutation::Unbind {
                expected_digest,
                expected_identity,
                ..
            } => {
                let identity = expected_identity.or(command.identity).ok_or_else(|| {
                    FilterError::new(FilterErrorReason::InvalidRequest, "no identity")
                })?;
                let binding = self.bindings.get(&identity).cloned().ok_or_else(|| {
                    FilterError::new(FilterErrorReason::NotFound, "the group is not bound")
                })?;
                if binding.digest != expected_digest {
                    return Err(FilterError::new(
                        FilterErrorReason::Conflict,
                        "bound to another digest",
                    ));
                }
                self.bindings.remove(&identity);
                self.bump(identity);
                Ok(FilterMutationResult::Unbound(binding))
            }
            FilterMutation::Drop { filter_id } => {
                if self.filters.remove(&filter_id).is_none() {
                    return Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        "no such filter",
                    ));
                }
                let released: Vec<FilterGroupIdentity> = self
                    .bindings
                    .iter()
                    .filter(|(_, binding)| binding.filter_id == filter_id)
                    .map(|(identity, _)| *identity)
                    .collect();
                for identity in released {
                    self.bindings.remove(&identity);
                    self.bump(identity);
                }
                self.owners.retain(|_, owned| *owned != filter_id);
                self.owned.remove(&filter_id);
                Ok(FilterMutationResult::Dropped { filter_id })
            }
            _ => Err(FilterError::new(
                FilterErrorReason::Unsupported,
                "the stand-in catalog serves group policies only",
            )),
        }
    }

    fn resolve(&self, request: &ResolveFilterPolicy) -> Result<FilterCatalogOutcome, FilterError> {
        let (filter_id, revision, policy_generation) = match request.policy {
            FilterPolicyRef::Binding(identity) => match self.bindings.get(&identity) {
                Some(binding) => (
                    binding.filter_id,
                    binding.revision,
                    binding.policy_generation,
                ),
                None if request.allow_unbound => {
                    return Ok(FilterCatalogOutcome::Unbound(GroupPolicyUnbound {
                        identity,
                        policy_generation: self.generations.get(&identity).copied().unwrap_or(0),
                    }));
                }
                None => {
                    return Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        "this consumer group has no filter binding",
                    ));
                }
            },
            FilterPolicyRef::Revision {
                filter_id,
                revision,
            } => (filter_id, revision, 0),
        };
        let info = self
            .filters
            .get(&filter_id)
            .and_then(|revisions| revisions.iter().find(|info| info.revision == revision))
            .ok_or_else(|| FilterError::new(FilterErrorReason::NotFound, "no such revision"))?;
        if !info.enabled && !request.allow_disabled {
            return Err(FilterError::new(
                FilterErrorReason::RevisionDisabled,
                "the revision is paused",
            ));
        }
        Ok(FilterCatalogOutcome::Policy(ResolvedFilterPolicy {
            filter_id,
            revision,
            digest: info.digest.clone(),
            state: FilterState::Active,
            filter: info.filter.clone(),
            policy_generation,
        }))
    }

    fn answer(&mut self, forwarded: &ForwardedCommand) -> Option<Vec<u8>> {
        let reply = match forwarded.command_code {
            AGDX_BACKEND_HELLO_CODE => {
                return encode_named(&BackendAnnounce::new(
                    OpVersions::new(
                        QUERY_OP_VERSION,
                        CONTROL_OP_VERSION,
                        KV_OP_VERSION,
                        FORK_OP_VERSION,
                    )
                    .with_filter(FILTER_OP_VERSION),
                ))
                .ok();
            }
            AGDX_FILTER_MUTATE_CODE => {
                let command: FilterCatalogCommand = decode_named(&forwarded.payload).ok()?;
                let operation_id = command.operation_id;
                let status = match self.mutate(command) {
                    Ok(result) => FilterMutationStatus::Applied(result),
                    Err(error) => FilterMutationStatus::Rejected(error),
                };
                self.offset += 1;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Mutation(
                    FilterMutationOutcome {
                        v: FILTER_OP_VERSION,
                        operation_id,
                        status,
                        catalog_position: Some(CatalogPosition {
                            partition_id: 1,
                            offset: self.offset,
                            operation_id: Some(operation_id),
                        }),
                    },
                )))
            }
            AGDX_RESOLVE_FILTER_POLICY_CODE => {
                let request: ResolveFilterPolicy = decode_named(&forwarded.payload).ok()?;
                match self.resolve(&request) {
                    Ok(outcome) => FilterCatalogReply::Ok(Box::new(outcome)),
                    Err(error) => FilterCatalogReply::Err(error),
                }
            }
            AGDX_GET_FILTER_BINDING_CODE => {
                let request: GetFilterBinding = decode_named(&forwarded.payload).ok()?;
                match request
                    .identity
                    .and_then(|identity| self.bindings.get(&identity))
                {
                    Some(binding) => FilterCatalogReply::Ok(Box::new(
                        FilterCatalogOutcome::Binding(binding.clone()),
                    )),
                    None => FilterCatalogReply::Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        "the group is not bound",
                    )),
                }
            }
            AGDX_LIST_FILTERS_CODE => {
                let request: ListFilters = decode_named(&forwarded.payload).ok()?;
                if let Some(owner) = request.owner {
                    self.owner_lookups.push(owner);
                    if self.owner_lookup_failures.remove(&owner) {
                        return encode_named(&FilterCatalogReply::Err(FilterError::new(
                            FilterErrorReason::CatalogUnavailable,
                            "the catalog is unavailable",
                        )))
                        .ok();
                    }
                }
                let mismatched = request
                    .owner
                    .is_some_and(|owner| self.owner_lookup_mismatches.contains(&owner));
                let mut items: Vec<FilterSummary> = self
                    .owned
                    .iter()
                    .filter(|(filter_id, owned)| {
                        let owner_matches = request
                            .owner
                            .is_none_or(|owner| (owned.owner.identity == owner) != mismatched);
                        owner_matches
                            && request.before_id.is_none_or(|before| **filter_id < before)
                            && request
                                .name_contains
                                .as_deref()
                                .is_none_or(|fragment| owned.name.contains(fragment))
                    })
                    .filter_map(|(filter_id, owned)| {
                        let latest = self.filters.get(filter_id)?.last()?;
                        Some(FilterSummary {
                            id: *filter_id,
                            name: owned.name.clone(),
                            description: String::new(),
                            state: FilterState::Active,
                            latest_revision: latest.revision,
                            latest_digest: latest.digest.clone(),
                            codec: latest.filter.codec,
                            bindings: self
                                .bindings
                                .values()
                                .filter(|binding| binding.filter_id == *filter_id)
                                .count() as u32,
                            created_at_micros: latest.created_at_micros,
                            updated_at_micros: latest.created_at_micros,
                            owner: Some(owned.owner.clone()),
                        })
                    })
                    .collect();
                items.sort_by_key(|summary| std::cmp::Reverse(summary.id));
                let total = items.len() as u32;
                let items = items
                    .into_iter()
                    .skip((request.page as usize).saturating_mul(request.page_size as usize))
                    .take(request.page_size as usize)
                    .collect();
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Filters(FilterPage {
                    items,
                    page: request.page,
                    page_size: request.page_size,
                    total,
                })))
            }
            AGDX_LIST_FILTER_REVISIONS_CODE => {
                let request: ListFilterRevisions = decode_named(&forwarded.payload).ok()?;
                let mut items = self
                    .filters
                    .get(&request.filter_id)
                    .cloned()
                    .unwrap_or_default();
                items.reverse();
                let total = items.len() as u32;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Revisions(
                    FilterRevisionPage {
                        filter_id: request.filter_id,
                        items,
                        page: request.page,
                        page_size: request.page_size,
                        total,
                    },
                )))
            }
            AGDX_LIST_FILTER_BINDINGS_CODE => {
                let request: ListFilterBindings = decode_named(&forwarded.payload).ok()?;
                let items: Vec<FilterBinding> = self
                    .bindings
                    .values()
                    .filter(|binding| {
                        request
                            .stream
                            .as_ref()
                            .is_none_or(|stream| *stream == binding.group.stream)
                            && request
                                .topic
                                .as_ref()
                                .is_none_or(|topic| *topic == binding.group.topic)
                    })
                    .cloned()
                    .collect();
                let total = items.len() as u32;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Bindings(
                    FilterBindingPage {
                        items,
                        page: request.page,
                        page_size: request.page_size,
                        total,
                    },
                )))
            }
            AGDX_FILTER_OPERATION_CODE => FilterCatalogReply::Err(FilterError::new(
                FilterErrorReason::NotFound,
                "the stand-in applies every mutation at once",
            )),
            _ => return None,
        };
        encode_named(&reply).ok()
    }
}

/// Serve the catalog on `socket` from a thread of its own, recording `node` for each forwarded command.
pub fn spawn(socket: &Path, state: Arc<Mutex<CatalogState>>, node: usize) -> std::io::Result<()> {
    let listener = std::os::unix::net::UnixListener::bind(socket)?;
    listener.set_nonblocking(true)?;
    // Its own runtime, because the catalog outlives the runtime of the test that starts it.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    std::thread::spawn(move || {
        runtime.block_on(async move {
            let Ok(listener) = UnixListener::from_std(listener) else {
                return;
            };
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve(socket, Arc::clone(&state), node));
            }
        });
    });
    Ok(())
}

async fn serve(mut socket: UnixStream, state: Arc<Mutex<CatalogState>>, node: usize) {
    loop {
        let mut length = [0u8; 4];
        if socket.read_exact(&mut length).await.is_err() {
            return;
        }
        let mut frame = vec![0u8; u32::from_le_bytes(length) as usize];
        if socket.read_exact(&mut frame).await.is_err() {
            return;
        }
        let Ok(forwarded) = decode_named::<ForwardedCommand>(&frame) else {
            return;
        };
        let reply = {
            let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
            state.forwarded.push((node, forwarded.command_code));
            state.answer(&forwarded)
        };
        let Some(reply) = reply else {
            return;
        };
        let Ok(reply_length) = u32::try_from(reply.len()) else {
            return;
        };
        let written = socket.write_all(&reply_length.to_le_bytes()).await;
        if written.is_err() || socket.write_all(&reply).await.is_err() {
            return;
        }
    }
}
