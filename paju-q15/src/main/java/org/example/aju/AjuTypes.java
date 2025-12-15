package org.example.aju;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;

/**
 * Core data types used by the AJU-based query processing engine.
 *
 * This file defines the common tuple representation, internal tuple records,
 * schema metadata, and the query plan abstraction used for incremental
 * maintenance of acyclic join queries.
 */
public class AjuTypes {

    /**
     * Relation identifiers used by the query engine.
     *
     * The enumeration covers all relations that may appear
     * in supported TPC-H queries.
     */
    public enum RelId {
        REGION,
        NATION,
        SUPPLIER,
        CUSTOMER,
        ORDERS,
        LINEITEM,
        PART
    }

    /**
     * Update operation type for streaming tuples.
     */
    public enum OpType {
        INSERT,
        DELETE
    }

    /**
     * Generic row representation for stream input and output.
     *
     * Each row corresponds to an insertion or deletion event
     * on a base relation or a derived join result.
     */
    public static class Row implements Serializable {
        public RelId rel;
        public OpType op;
        public Map<String, Object> fields = new HashMap<>();

        public Row() {}

        public Row(RelId rel, OpType op) {
            this.rel = rel;
            this.op = op;
        }

        public <T> T get(String name) {
            return (T) fields.get(name);
        }

        public Row set(String name, Object value) {
            fields.put(name, value);
            return this;
        }

        @Override
        public String toString() {
            return "Row{" +
                    "rel=" + rel +
                    ", op=" + op +
                    ", fields=" + fields +
                    '}';
        }
    }

    /**
     * Metadata for assertion keys.
     *
     * Assertion keys are used to enforce consistency constraints
     * across multiple join paths in more complex join graphs.
     */
    public static class AssertionKeyMeta implements Serializable {
        public final RelId rootRel;
        public final String attrName;
        public final Set<RelId> carrierChildren = EnumSet.noneOf(RelId.class);

        public AssertionKeyMeta(RelId rootRel, String attrName) {
            this.rootRel = rootRel;
            this.attrName = attrName;
        }

        public AssertionKeyMeta addCarrier(RelId child) {
            carrierChildren.add(child);
            return this;
        }
    }

    /**
     * Schema metadata for a relation.
     *
     * This structure captures primary key information,
     * foreign-key relationships, and the parent–child
     * structure of the join DAG.
     */
    public static class RelationMeta implements Serializable {
        public final RelId id;
        public final String pkAttr;

        public final List<RelId> children = new ArrayList<>();
        public final List<RelId> parents = new ArrayList<>();
        public final Map<RelId, String> fkToParent = new HashMap<>();

        public final List<AssertionKeyMeta> assertionKeys = new ArrayList<>();

        public RelationMeta(RelId id, String pkAttr) {
            this.id = id;
            this.pkAttr = pkAttr;
        }

        public RelationMeta addParent(RelId parentRel, String fkAttr) {
            parents.add(parentRel);
            fkToParent.put(parentRel, fkAttr);
            return this;
        }

        public RelationMeta addChild(RelId child) {
            children.add(child);
            return this;
        }

        public RelationMeta addAssertion(AssertionKeyMeta ak) {
            assertionKeys.add(ak);
            return this;
        }
    }

    /**
     * Internal tuple representation maintained by the engine.
     *
     * Each TupleRecord stores the tuple attributes, its primary key,
     * the current satisfaction counter s(t), and its liveness state.
     */
    public static class TupleRecord implements Serializable {
        public final RelId rel;
        public final Object pk;
        public final Map<String, Object> attrs = new HashMap<>();

        public int s = 0;
        public boolean live = false;

        public TupleRecord(RelId rel, Object pk, Map<String, Object> attrs) {
            this.rel = rel;
            this.pk = pk;
            if (attrs != null) this.attrs.putAll(attrs);
        }

        public <T> T get(String name) {
            return (T) attrs.get(name);
        }

        public void set(String name, Object value) {
            attrs.put(name, value);
        }
    }

    /**
     * Query plan abstraction.
     *
     * A concrete query (e.g., a TPC-H query) implements this interface
     * to define selection predicates, join roots, and output construction.
     */
    public interface QueryPlan extends Serializable {

        Set<RelId> usedRelations();

        RelId root();

        boolean passesSelection(Row base);

        Row buildOutput(Map<RelId, TupleRecord> joined);
    }

    // ============================================================
    // BigDecimal utility functions
    // ============================================================

    public static BigDecimal mul(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) return BigDecimal.ZERO;
        return a.multiply(b);
    }

    public static BigDecimal oneMinus(BigDecimal x) {
        if (x == null) return BigDecimal.ONE;
        return BigDecimal.ONE.subtract(x);
    }
}
