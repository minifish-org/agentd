use super::*;

#[cfg(test)]
#[derive(Clone, Default)]
pub(super) struct MemorySearchHook {
    pub snapshot_ready: Arc<tokio::sync::Notify>,
    pub writer_finished: Arc<tokio::sync::Notify>,
}

impl AgentdStore {
    pub async fn get_memory(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
    ) -> Result<Option<MemoryItem>> {
        let row = db::query(
            "SELECT tenant, namespace, id, text, created_at, updated_at \
             FROM memory WHERE tenant = ? AND namespace = ? AND id = ?",
        )
        .bind(tenant)
        .bind(normalize_memory_component(namespace, "namespace")?)
        .bind(normalize_memory_component(id, "id")?)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| memory_item_from_row(row, None)).transpose()
    }

    pub async fn list_memory_page(
        &self,
        tenant: &str,
        namespace: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<MemoryPage> {
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let after_id = after_id
            .map(|id| normalize_memory_component(id, "cursor id"))
            .transpose()?;
        let limit = limit.clamp(1, 100);
        let rows = if let Some(after_id) = after_id.as_deref() {
            db::query(
                "SELECT id, text, created_at, updated_at FROM memory \
                 WHERE tenant = ? AND namespace = ? AND id > ? \
                 ORDER BY id ASC LIMIT ?",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(after_id)
            .bind((limit + 1) as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT id, text, created_at, updated_at FROM memory \
                 WHERE tenant = ? AND namespace = ? \
                 ORDER BY id ASC LIMIT ?",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind((limit + 1) as i64)
            .fetch_all(&self.pool)
            .await?
        };
        let has_more = rows.len() > limit;
        let items = rows
            .into_iter()
            .take(limit)
            .map(|row| {
                Ok(MemoryListItem {
                    id: row.try_get("id")?,
                    text: row.try_get("text")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let next_after_id = has_more
            .then(|| items.last().map(|item| item.id.clone()))
            .flatten();
        Ok(MemoryPage {
            items,
            next_after_id,
        })
    }

    pub async fn search_memory(
        &self,
        tenant: &str,
        namespace: &str,
        query: &str,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<MemoryItem>> {
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let query = query.trim();
        if query.is_empty() {
            return Err(invalid!("memory query is required"));
        }
        validate_memory_embedding(query_embedding)?;
        let limit = limit.clamp(1, 20);
        let candidate_limit = limit.saturating_mul(4);
        // A dedicated WAL read transaction keeps lexical results and every
        // semantic batch on one snapshot without holding the shared reader.
        let mut tx = self.pool.begin().await?;

        let lexical = if let Some(fts_query) = memory_fts_query(query) {
            let rows = db::query(
                "SELECT m.tenant, m.namespace, m.id, m.text, m.created_at, m.updated_at, \
                        bm25(memory_fts) AS lexical_rank \
                 FROM memory_fts JOIN memory m ON m.rowid = memory_fts.rowid \
                 WHERE memory_fts MATCH ? AND m.tenant = ? AND m.namespace = ? \
                 ORDER BY lexical_rank, m.id LIMIT ?",
            )
            .bind(fts_query)
            .bind(tenant)
            .bind(&namespace)
            .bind(candidate_limit as i64)
            .fetch_all(&mut tx)
            .await?;
            rows.into_iter()
                .map(|row| memory_item_from_row(row, None))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };

        // Bound each read and ranking heap while retaining the snapshot.
        let mut semantic = BinaryHeap::with_capacity(candidate_limit.saturating_add(1));
        let mut after_id = String::new();
        loop {
            let rows = db::query(
                "SELECT tenant, namespace, id, text, embedding, created_at, updated_at \
                 FROM memory WHERE tenant = ? AND namespace = ? AND id > ? ORDER BY id LIMIT 256",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(&after_id)
            .fetch_all(&mut tx)
            .await?;
            let full_batch = rows.len() == 256;
            for row in rows {
                after_id = row.try_get("id")?;
                let stored_embedding =
                    decode_memory_embedding(&row.try_get::<Vec<u8>, _>("embedding")?)?;
                semantic.push(Reverse(SemanticMemoryHit {
                    similarity: cosine_similarity(query_embedding, &stored_embedding),
                    item: memory_item_from_row(row, None)?,
                }));
                if semantic.len() > candidate_limit {
                    semantic.pop();
                }
            }
            #[cfg(test)]
            {
                let hook = self.memory_search_hook.lock().await.take();
                if let Some(hook) = hook {
                    hook.snapshot_ready.notify_one();
                    hook.writer_finished.notified().await;
                }
            }
            if !full_batch {
                break;
            }
            tokio::task::yield_now().await;
        }
        tx.commit().await?;
        let mut semantic = semantic
            .into_iter()
            .map(|Reverse(hit)| hit)
            .collect::<Vec<_>>();
        semantic.sort_by(|left, right| {
            right
                .similarity
                .total_cmp(&left.similarity)
                .then_with(|| left.item.id.cmp(&right.item.id))
        });

        Ok(fuse_memory_candidates(
            lexical,
            semantic.into_iter().map(|hit| hit.item).collect(),
            limit,
        ))
    }

    pub async fn put_memory(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
        text: &str,
        embedding: &[f32],
    ) -> Result<MemoryItem> {
        self.put_memory_with_graph(
            tenant,
            namespace,
            id,
            text,
            embedding,
            &MemoryGraphInput::default(),
        )
        .await
    }

    pub async fn put_memory_with_graph(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
        text: &str,
        embedding: &[f32],
        graph: &MemoryGraphInput,
    ) -> Result<MemoryItem> {
        self.put_memory_with_source((tenant, None), namespace, id, text, embedding, graph)
            .await
    }

    /// Runtime writes carry a trusted run ID, never a model-supplied actor.
    pub async fn put_memory_with_graph_for_run(
        &self,
        run_id: Uuid,
        namespace: &str,
        id: &str,
        text: &str,
        embedding: &[f32],
        graph: &MemoryGraphInput,
    ) -> Result<MemoryItem> {
        let run = self
            .get_run(run_id)
            .await?
            .ok_or_else(|| missing!("memory writer run not found"))?;
        with_audit_context(
            AuditContext::agent(&run.agent_ref, run_id),
            self.put_memory_with_source(
                (&run.tenant, Some(run_id)),
                namespace,
                id,
                text,
                embedding,
                graph,
            ),
        )
        .await
    }

    pub(super) async fn put_memory_with_source(
        &self,
        source: (&str, Option<Uuid>),
        namespace: &str,
        id: &str,
        text: &str,
        embedding: &[f32],
        graph: &MemoryGraphInput,
    ) -> Result<MemoryItem> {
        let (tenant, source_run_id) = source;
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let id = normalize_memory_component(id, "id")?;
        let text = text.trim();
        if text.is_empty() {
            return Err(invalid!("memory text is required"));
        }
        if text.len() > MAX_MEMORY_TEXT_BYTES {
            return Err(invalid!(
                "memory text exceeds {MAX_MEMORY_TEXT_BYTES} UTF-8 bytes; store long content as an artifact"
            ));
        }
        validate_memory_embedding(embedding)?;
        let graph = normalize_memory_graph(graph)?;
        let embedding = encode_memory_embedding(embedding);
        let now = Utc::now().to_rfc3339();
        let mut tx = self.begin_tenant_write(tenant).await?;
        let previous_text = db::query_scalar::<String>(
            "SELECT text FROM memory WHERE tenant = ? AND namespace = ? AND id = ?",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(&id)
        .fetch_optional(&mut tx)
        .await?;
        let content_changed = previous_text.as_deref() != Some(text);
        db::query(
            "INSERT INTO memory (tenant, namespace, id, text, embedding, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(tenant, namespace, id) DO UPDATE SET \
             text = excluded.text, embedding = excluded.embedding, updated_at = excluded.updated_at",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(&id)
        .bind(text)
        .bind(embedding)
        .bind(&now)
        .bind(&now)
        .execute(&mut tx)
        .await?;

        db::query("DELETE FROM edges WHERE tenant = ? AND namespace = ? AND memory_id = ?")
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .execute(&mut tx)
            .await?;
        db::query("DELETE FROM entities WHERE tenant = ? AND namespace = ? AND memory_id = ?")
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .execute(&mut tx)
            .await?;

        for entity in &graph.entities {
            db::query(
                "INSERT INTO entities (tenant, namespace, memory_id, entity_id, label, entity_type, properties_json, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .bind(&entity.id)
            .bind(&entity.label)
            .bind(&entity.kind)
            .bind(serde_json::to_string(&entity.properties)?)
            .bind(&now)
            .bind(&now)
            .execute(&mut tx)
            .await?;
        }
        for edge in &graph.edges {
            db::query(
                "INSERT INTO edges (tenant, namespace, memory_id, source_entity_id, relation, target_entity_id, properties_json, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .bind(&edge.from)
            .bind(&edge.relation)
            .bind(&edge.to)
            .bind(serde_json::to_string(&edge.properties)?)
            .bind(&now)
            .bind(&now)
            .execute(&mut tx)
            .await?;
        }
        maintenance::record_memory_change(
            &mut tx,
            tenant,
            &namespace,
            source_run_id,
            content_changed,
        )
        .await?;
        let mut event = AuditInput::new(
            Some(tenant),
            "memory.put",
            "memory",
            Some(&id),
            "succeeded",
            json!({"namespace":namespace,"created":previous_text.is_none(),"text_changed":content_changed,"text_bytes":text.len(),"graph_entities":graph.entities.len(),"graph_edges":graph.edges.len()}),
        );
        event.run_id = source_run_id;
        audit::record(&mut tx, event).await?;
        tx.commit().await?;
        self.get_memory(tenant, &namespace, &id)
            .await?
            .ok_or_else(|| anyhow!("memory was not readable after put"))
    }

    pub async fn delete_memory(&self, tenant: &str, namespace: &str, id: &str) -> Result<bool> {
        self.delete_memory_with_source(tenant, namespace, id, None)
            .await
    }

    pub async fn delete_memory_for_run(
        &self,
        run_id: Uuid,
        namespace: &str,
        id: &str,
    ) -> Result<bool> {
        let run = self
            .get_run(run_id)
            .await?
            .ok_or_else(|| missing!("memory writer run not found"))?;
        with_audit_context(
            AuditContext::agent(&run.agent_ref, run_id),
            self.delete_memory_with_source(&run.tenant, namespace, id, Some(run_id)),
        )
        .await
    }

    pub(super) async fn delete_memory_with_source(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
        source_run_id: Option<Uuid>,
    ) -> Result<bool> {
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let id = normalize_memory_component(id, "id")?;
        let mut tx = self.pool.begin().await?;
        let deleted = db::query("DELETE FROM memory WHERE tenant = ? AND namespace = ? AND id = ?")
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .execute(&mut tx)
            .await?
            .rows_affected();
        maintenance::record_memory_change(&mut tx, tenant, &namespace, source_run_id, deleted > 0)
            .await?;
        let mut event = AuditInput::new(
            Some(tenant),
            "memory.delete",
            "memory",
            Some(&id),
            audit_mutations::outcome(deleted > 0),
            json!({"namespace":namespace,"deleted":deleted > 0}),
        );
        event.run_id = source_run_id;
        audit::record(&mut tx, event).await?;
        tx.commit().await?;
        Ok(deleted > 0)
    }

    pub async fn query_graph(
        &self,
        tenant: &str,
        namespace: &str,
        query: GraphQuery<'_>,
    ) -> Result<GraphQueryResult> {
        let GraphQuery {
            entity,
            relation,
            direction,
            max_hops,
            limit,
        } = query;
        self.ensure_tenant_exists(tenant).await?;
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let entity = normalize_graph_component(entity, "entity", MAX_GRAPH_LABEL_BYTES)?;
        let relation = relation
            .map(|value| normalize_graph_component(value, "relation", MAX_GRAPH_RELATION_BYTES))
            .transpose()?;
        if !matches!(direction, "outgoing" | "incoming" | "both") {
            return Err(invalid!(
                "graph direction must be outgoing, incoming, or both"
            ));
        }
        let max_hops = max_hops.clamp(1, 3) as i64;
        let limit = limit.clamp(1, 100) as i64;

        let start_ids: BTreeSet<String> = db::query_scalar(
            "SELECT DISTINCT entity_id FROM entities \
             WHERE tenant = ? AND namespace = ? \
               AND (entity_id = ? COLLATE NOCASE OR label = ? COLLATE NOCASE) \
             ORDER BY entity_id ASC LIMIT 16",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(&entity)
        .bind(&entity)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect();
        if start_ids.is_empty() {
            return Ok(GraphQueryResult {
                entities: Vec::new(),
                paths: Vec::new(),
            });
        }

        let rows = db::query(
            r#"WITH RECURSIVE
               start_nodes(entity_id) AS (
                   SELECT value FROM json_each(?)
               ),
               adjacency(from_id, to_id, edge_from, edge_to, relation, memory_id, properties_json) AS (
                   SELECT source_entity_id, target_entity_id, source_entity_id, target_entity_id,
                          relation, memory_id, properties_json
                   FROM edges
                   WHERE tenant = ? AND namespace = ? AND ? IN ('outgoing', 'both')
                     AND (? IS NULL OR relation = ?)
                   UNION ALL
                   SELECT target_entity_id, source_entity_id, source_entity_id, target_entity_id,
                          relation, memory_id, properties_json
                   FROM edges
                   WHERE tenant = ? AND namespace = ? AND ? IN ('incoming', 'both')
                     AND (? IS NULL OR relation = ?)
               ),
               walk(depth, current_id, visited, node_path, edge_path) AS (
                   SELECT 0, entity_id, char(31) || entity_id || char(31),
                          json_array(entity_id), json_array()
                   FROM start_nodes
                   UNION ALL
                   SELECT walk.depth + 1,
                          adjacency.to_id,
                          walk.visited || adjacency.to_id || char(31),
                          json_insert(walk.node_path, '$[#]', adjacency.to_id),
                          json_insert(
                              walk.edge_path,
                              '$[#]',
                              json_object(
                                  'from', adjacency.edge_from,
                                  'relation', adjacency.relation,
                                  'to', adjacency.edge_to,
                                  'memory_id', adjacency.memory_id,
                                  'properties', json(adjacency.properties_json)
                              )
                          )
                   FROM walk
                   JOIN adjacency ON adjacency.from_id = walk.current_id
                   WHERE walk.depth < ?
                     AND instr(
                         walk.visited,
                         char(31) || adjacency.to_id || char(31)
                     ) = 0
                   LIMIT ?
               )
               SELECT depth, node_path, edge_path
               FROM walk
               WHERE depth > 0
               ORDER BY depth ASC, current_id ASC, node_path ASC
               LIMIT ?"#,
        )
        .bind(serde_json::to_string(&start_ids)?)
        .bind(tenant)
        .bind(&namespace)
        .bind(direction)
        .bind(&relation)
        .bind(&relation)
        .bind(tenant)
        .bind(&namespace)
        .bind(direction)
        .bind(&relation)
        .bind(&relation)
        .bind(max_hops)
        .bind(MAX_GRAPH_WALK_ROWS)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut paths = Vec::with_capacity(rows.len());
        let mut used_entity_ids = start_ids;
        for row in rows {
            let nodes: Vec<String> = decode_json(&row.try_get::<String, _>("node_path")?)?;
            let edges: Vec<GraphPathEdge> = decode_json(&row.try_get::<String, _>("edge_path")?)?;
            used_entity_ids.extend(nodes.iter().cloned());
            paths.push(GraphPath {
                hops: row.try_get::<i64, _>("depth")? as usize,
                nodes,
                edges,
            });
        }
        let entity_rows = db::query(
            "SELECT memory_id, entity_id, label, entity_type, properties_json, updated_at \
             FROM entities WHERE tenant = ? AND namespace = ? \
               AND entity_id IN (SELECT value FROM json_each(?)) \
             ORDER BY updated_at DESC, memory_id ASC",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(serde_json::to_string(&used_entity_ids)?)
        .fetch_all(&self.pool)
        .await?;
        let mut entity_index = BTreeMap::<String, GraphEntity>::new();
        for row in entity_rows {
            let entity_id = row.try_get::<String, _>("entity_id")?;
            let memory_id = row.try_get::<String, _>("memory_id")?;
            if let Some(existing) = entity_index.get_mut(&entity_id) {
                if !existing.memory_ids.contains(&memory_id) {
                    existing.memory_ids.push(memory_id);
                }
                continue;
            }
            entity_index.insert(
                entity_id.clone(),
                GraphEntity {
                    id: entity_id,
                    label: row.try_get("label")?,
                    kind: row.try_get("entity_type")?,
                    properties: decode_json(&row.try_get::<String, _>("properties_json")?)?,
                    memory_ids: vec![memory_id],
                },
            );
        }
        let entities = entity_index.into_values().collect();
        Ok(GraphQueryResult { entities, paths })
    }
}

pub(super) fn normalize_memory_component(value: &str, field: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid!("memory {field} is required"));
    }
    Ok(value.to_string())
}

pub(super) fn normalize_graph_component(
    value: &str,
    field: &str,
    max_bytes: usize,
) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid!("graph {field} is required"));
    }
    if value.len() > max_bytes {
        return Err(invalid!("graph {field} exceeds {max_bytes} UTF-8 bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid!("graph {field} contains a control character"));
    }
    Ok(value.to_string())
}

pub(super) fn normalize_graph_properties(
    properties: &serde_json::Value,
    field: &str,
) -> Result<serde_json::Value> {
    if !properties.is_object() {
        return Err(invalid!("graph {field} properties must be an object"));
    }
    let encoded = serde_json::to_vec(properties)?;
    if encoded.len() > MAX_GRAPH_PROPERTIES_BYTES {
        return Err(invalid!(
            "graph {field} properties exceed {MAX_GRAPH_PROPERTIES_BYTES} JSON bytes"
        ));
    }
    Ok(properties.clone())
}

pub(super) fn normalize_memory_graph(graph: &MemoryGraphInput) -> Result<MemoryGraphInput> {
    if graph.entities.len() > MAX_GRAPH_ENTITIES_PER_MEMORY {
        return Err(invalid!(
            "memory graph exceeds {MAX_GRAPH_ENTITIES_PER_MEMORY} entities"
        ));
    }
    if graph.edges.len() > MAX_GRAPH_EDGES_PER_MEMORY {
        return Err(invalid!(
            "memory graph exceeds {MAX_GRAPH_EDGES_PER_MEMORY} edges"
        ));
    }

    let mut entity_ids = BTreeSet::new();
    let mut entities = Vec::with_capacity(graph.entities.len());
    for entity in &graph.entities {
        let id = normalize_graph_component(&entity.id, "entity id", MAX_GRAPH_ID_BYTES)?;
        if !entity_ids.insert(id.clone()) {
            return Err(invalid!("memory graph contains duplicate entity id {id}"));
        }
        let label =
            normalize_graph_component(&entity.label, "entity label", MAX_GRAPH_LABEL_BYTES)?;
        let kind = entity
            .kind
            .as_deref()
            .map(|value| normalize_graph_component(value, "entity type", MAX_GRAPH_ID_BYTES))
            .transpose()?;
        entities.push(GraphEntityInput {
            id,
            label,
            kind,
            properties: normalize_graph_properties(&entity.properties, "entity")?,
        });
    }

    let mut edge_keys = BTreeSet::new();
    let mut edges = Vec::with_capacity(graph.edges.len());
    for edge in &graph.edges {
        let from = normalize_graph_component(&edge.from, "edge from", MAX_GRAPH_ID_BYTES)?;
        let relation =
            normalize_graph_component(&edge.relation, "edge relation", MAX_GRAPH_RELATION_BYTES)?;
        let to = normalize_graph_component(&edge.to, "edge to", MAX_GRAPH_ID_BYTES)?;
        if !entity_ids.contains(&from) || !entity_ids.contains(&to) {
            return Err(invalid!(
                "memory graph edge {from} -[{relation}]-> {to} must reference entities in the same memory graph"
            ));
        }
        if !edge_keys.insert((from.clone(), relation.clone(), to.clone())) {
            return Err(invalid!(
                "memory graph contains duplicate edge {from} -[{relation}]-> {to}"
            ));
        }
        edges.push(GraphEdgeInput {
            from,
            relation,
            to,
            properties: normalize_graph_properties(&edge.properties, "edge")?,
        });
    }
    Ok(MemoryGraphInput { entities, edges })
}

/// Shared by capability parsing and persistence, so offline validation rejects
/// the same graph shapes before embedding or database work begins.
pub fn validate_memory_graph_input(graph: &MemoryGraphInput) -> Result<()> {
    normalize_memory_graph(graph).map(|_| ())
}

pub(super) fn memory_fts_query(query: &str) -> Option<String> {
    let terms = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| format!("\"{term}\""))
        .collect::<Vec<_>>();
    (!terms.is_empty()).then(|| terms.join(" OR "))
}

pub(super) fn memory_item_from_row(row: db::SqlRow, score: Option<f64>) -> Result<MemoryItem> {
    Ok(MemoryItem {
        tenant: row.try_get("tenant")?,
        namespace: row.try_get("namespace")?,
        id: row.try_get("id")?,
        text: row.try_get("text")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        score,
    })
}

pub(super) fn validate_memory_embedding(embedding: &[f32]) -> Result<()> {
    if embedding.len() != MEMORY_EMBEDDING_DIM {
        return Err(invalid!(
            "memory embedding must contain exactly {MEMORY_EMBEDDING_DIM} dimensions, got {}",
            embedding.len()
        ));
    }
    if embedding.iter().any(|value| !value.is_finite()) {
        return Err(invalid!("memory embedding contains a non-finite value"));
    }
    Ok(())
}

pub(super) fn encode_memory_embedding(embedding: &[f32]) -> Vec<u8> {
    embedding
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

pub(super) fn decode_memory_embedding(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(std::mem::size_of::<f32>()) {
        return Err(memory_embedding_reset_error(format!(
            "stored vector has invalid byte length {}",
            bytes.len()
        )));
    }
    let embedding = bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect::<Vec<_>>();
    validate_memory_embedding(&embedding)
        .map_err(|error| memory_embedding_reset_error(error.to_string()))?;
    Ok(embedding)
}

pub(super) fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (left, right) in left.iter().zip(right) {
        let left = f64::from(*left);
        let right = f64::from(*right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm.sqrt() * right_norm.sqrt())
    }
}

pub(super) fn fuse_memory_candidates(
    lexical: Vec<MemoryItem>,
    semantic: Vec<MemoryItem>,
    limit: usize,
) -> Vec<MemoryItem> {
    const RRF_K: f64 = 60.0;
    let mut fused: BTreeMap<String, (MemoryItem, f64)> = BTreeMap::new();
    for candidates in [lexical, semantic] {
        for (offset, item) in candidates.into_iter().enumerate() {
            let contribution = 1.0 / (RRF_K + (offset + 1) as f64);
            let entry = fused.entry(item.id.clone()).or_insert((item, 0.0));
            entry.1 += contribution;
        }
    }
    let mut fused = fused
        .into_values()
        .map(|(mut item, score)| {
            item.score = Some(score);
            item
        })
        .collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .score
            .unwrap_or_default()
            .total_cmp(&left.score.unwrap_or_default())
            .then_with(|| left.id.cmp(&right.id))
    });
    fused.truncate(limit);
    fused
}

pub(super) fn memory_embedding_reset_error(detail: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("memory embedding is incompatible ({detail}); restart agentd with --reset-data")
}
