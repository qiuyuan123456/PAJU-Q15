package org.example.aju;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;

/**
 * AJU 基础类型：Row、TupleRecord、Schema 元信息、QueryPlan。
 * 适配 TPC-H Q8。
 */
public class AjuTypes {

    /** 关系 ID（必须覆盖 Q8 所有表） */
    public enum RelId {
        REGION,
        NATION,
        SUPPLIER,
        CUSTOMER,
        ORDERS,
        LINEITEM,
        PART
    }

    /** 操作类型 */
    public enum OpType {
        INSERT,
        DELETE
    }

    /** 流输入/输出的通用行结构 */
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

    /** Assertion key 元数据（可用于多路径一致性） */
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

    /** Schema 元信息 */
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

    /** 引擎内部存储的 tuple */
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

    /** QueryPlan 接口（Q8 会实现） */
    public interface QueryPlan extends Serializable {

        Set<RelId> usedRelations();

        RelId root();

        boolean passesSelection(Row base);

        Row buildOutput(Map<RelId, TupleRecord> joined);
    }

    // ===== BigDecimal 工具 =====
    public static BigDecimal mul(BigDecimal a, BigDecimal b) {
        if (a == null || b == null) return BigDecimal.ZERO;
        return a.multiply(b);
    }

    public static BigDecimal oneMinus(BigDecimal x) {
        if (x == null) return BigDecimal.ONE;
        return BigDecimal.ONE.subtract(x);
    }
}
