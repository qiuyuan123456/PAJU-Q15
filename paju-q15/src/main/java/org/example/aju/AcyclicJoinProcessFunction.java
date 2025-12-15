package org.example.aju;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import static org.example.aju.AjuTypes.*;

/**
 * Core processing function of the AJU-based engine.
 *
 * This operator incrementally maintains the join results of an
 * acyclic foreign-key join DAG under insertions and deletions.
 * The implementation follows the liveness-based maintenance
 * strategy and is instantiated for a fixed query plan.
 */
public class AcyclicJoinProcessFunction
        extends KeyedProcessFunction<Integer, Row, Row> {

    private final Map<RelId, RelationMeta> schema;
    private final QueryPlan query;

    /**
     * Per-relation state used to maintain tuple liveness.
     *
     * Live tuples participate in at least one valid join result,
     * while non-live tuples do not currently contribute to any
     * complete join result.
     */
    private static class RelationState {
        Map<Object, TupleRecord> live = new HashMap<>();
        Map<Object, TupleRecord> nonLive = new HashMap<>();
    }

    private final Map<RelId, RelationState> relState =
            new EnumMap<>(RelId.class);

    /**
     * Adjacency index for maintaining parent–child relationships.
     *
     * Structure:
     *   parentRel → childRel → parentPK → set of child tuples
     *
     * This index is used to efficiently propagate liveness
     * changes along the join DAG.
     */
    private final Map<RelId, Map<RelId, Map<Object, Set<TupleRecord>>>> edgeIndex =
            new EnumMap<>(RelId.class);

    public AcyclicJoinProcessFunction(Map<RelId, RelationMeta> schema,
                                      QueryPlan query) {
        this.schema = schema;
        this.query = query;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {

        // Initialize per-relation state and adjacency indices
        for (RelationMeta rm : schema.values()) {

            relState.put(rm.id, new RelationState());

            Map<RelId, Map<Object, Set<TupleRecord>>> childMap =
                    new HashMap<>();

            for (RelId child : rm.children) {
                childMap.put(child, new HashMap<>());
            }

            edgeIndex.put(rm.id, childMap);
        }
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Row> out) {
        if (row.op == OpType.INSERT)
            handleInsert(row, out);
        else
            handleDelete(row, out);
    }

    // ============================================================
    // INSERT handling
    // ============================================================
    private void handleInsert(Row row, Collector<Row> out) {

        RelationMeta meta = schema.get(row.rel);
        if (meta == null) return;

        RelationState rs = relState.get(row.rel);

        Object pk = row.get(meta.pkAttr);
        if (pk == null) return;

        TupleRecord rec =
                rs.live.get(pk) != null ? rs.live.get(pk) :
                        rs.nonLive.get(pk);

        if (rec == null) {
            // New tuple
            rec = new TupleRecord(row.rel, pk, row.fields);
            rs.nonLive.put(pk, rec);

            rec.s = computeInitialS(rec);

            if (shouldBeLive(rec))
                setLive(rec, true, out);

        } else {
            // Existing tuple update
            rec.attrs.putAll(row.fields);

            boolean want = shouldBeLive(rec);
            if (!rec.live && want)
                setLive(rec, true, out);
            else if (rec.live && !want)
                setLive(rec, false, out);
        }
    }

    // ============================================================
    // DELETE handling
    // ============================================================
    private void handleDelete(Row row, Collector<Row> out) {

        RelationMeta meta = schema.get(row.rel);
        if (meta == null) return;

        RelationState rs = relState.get(row.rel);

        Object pk = row.get(meta.pkAttr);
        if (pk == null) return;

        TupleRecord rec =
                rs.live.get(pk) != null ? rs.live.get(pk) :
                        rs.nonLive.get(pk);

        if (rec == null) return;

        if (rec.live)
            setLive(rec, false, out);

        rs.live.remove(pk);
        rs.nonLive.remove(pk);
    }

    // ============================================================
    // Initial s(t) computation
    // ============================================================
    private int computeInitialS(TupleRecord rec) {

        RelationMeta meta = schema.get(rec.rel);
        if (meta.children.isEmpty()) return 0;

        Map<RelId, Map<Object, Set<TupleRecord>>> children =
                edgeIndex.get(rec.rel);

        int cnt = 0;

        for (RelId c : meta.children) {
            Map<Object, Set<TupleRecord>> m = children.get(c);
            if (m == null) continue;

            Set<TupleRecord> set = m.get(rec.pk);
            if (set != null && !set.isEmpty())
                cnt++;
        }
        return cnt;
    }

    // ============================================================
    // Liveness transition
    // ============================================================
    private void setLive(TupleRecord rec,
                         boolean newLive,
                         Collector<Row> out) {

        RelationState rs = relState.get(rec.rel);

        if (rec.live == newLive) return;

        if (newLive) {
            rs.nonLive.remove(rec.pk);
            rs.live.put(rec.pk, rec);
        } else {
            rs.live.remove(rec.pk);
            rs.nonLive.put(rec.pk, rec);
        }
        rec.live = newLive;

        updateParents(rec, newLive, out);

        // Emit join result changes at the root relation
        if (query != null && rec.rel == query.root()) {
            Map<RelId, TupleRecord> join = buildJoin(rec);
            Row result = query.buildOutput(join);

            if (result != null) {
                result.op = newLive ? OpType.INSERT : OpType.DELETE;
                out.collect(result);
            }
        }
    }

    // ============================================================
    // Upward propagation
    // ============================================================
    private void updateParents(TupleRecord child,
                               boolean live,
                               Collector<Row> out) {

        RelationMeta meta = schema.get(child.rel);

        for (RelId parentRel : meta.parents) {

            String fk = meta.fkToParent.get(parentRel);
            Object parentPk = child.get(fk);
            if (parentPk == null) continue;

            Map<RelId, Map<Object, Set<TupleRecord>>> childMap =
                    edgeIndex.get(parentRel);

            Map<Object, Set<TupleRecord>> bucketMap =
                    childMap.computeIfAbsent(child.rel, x -> new HashMap<>());

            Set<TupleRecord> bucket =
                    bucketMap.computeIfAbsent(parentPk, x -> new HashSet<>());

            boolean before = !bucket.isEmpty();

            if (live) bucket.add(child);
            else bucket.remove(child);

            boolean after = !bucket.isEmpty();

            RelationState prs = relState.get(parentRel);
            TupleRecord parent =
                    prs.live.get(parentPk) != null ?
                            prs.live.get(parentPk) :
                            prs.nonLive.get(parentPk);

            if (parent == null) continue;

            boolean changed = false;

            if (!before && after) {
                parent.s++;
                changed = true;
            } else if (before && !after) {
                parent.s--;
                changed = true;
            }

            if (changed) {
                boolean want = shouldBeLive(parent);
                if (!parent.live && want)
                    setLive(parent, true, out);
                else if (parent.live && !want)
                    setLive(parent, false, out);
            }
        }
    }

    // ============================================================
    // Liveness condition
    // ============================================================
    private boolean shouldBeLive(TupleRecord rec) {

        RelationMeta meta = schema.get(rec.rel);

        Row tmp = new Row();
        tmp.rel = rec.rel;
        tmp.fields = rec.attrs;

        if (!query.passesSelection(tmp))
            return false;

        return rec.s == meta.children.size();
    }

    // ============================================================
    // Join materialization
    // ============================================================
    private Map<RelId, TupleRecord> buildJoin(TupleRecord root) {

        Map<RelId, TupleRecord> map =
                new EnumMap<>(RelId.class);

        dfs(root, map);
        return map;
    }

    private void dfs(TupleRecord rec,
                     Map<RelId, TupleRecord> map) {

        if (map.containsKey(rec.rel)) return;
        map.put(rec.rel, rec);

        RelationMeta meta = schema.get(rec.rel);

        Map<RelId, Map<Object, Set<TupleRecord>>> children =
                edgeIndex.get(rec.rel);

        for (RelId c : meta.children) {

            Map<Object, Set<TupleRecord>> m = children.get(c);
            if (m == null) continue;

            Set<TupleRecord> set = m.get(rec.pk);
            if (set == null || set.isEmpty()) continue;

            TupleRecord next = set.iterator().next();
            dfs(next, map);
        }
    }
}
