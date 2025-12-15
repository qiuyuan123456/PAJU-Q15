package org.example.aju;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.example.aju.AjuTypes.*;

/**
 * TPC-H Query 15 (Top Supplier) — 基础增量版
 *
 * Join DAG:
 *   SUPPLIER ← LINEITEM
 *
 * Output fields (per tuple):
 *   s_suppkey
 *   s_name
 *   s_address
 *   s_phone
 *   revenue = l_extendedprice * (1 - l_discount)
 *
 * 聚合与 max 选择在 Q15Aggregation / Q15MaxAggregation 中进行
 */
public class Q15Plan implements QueryPlan {

    /** Q15 shipdate 窗口 */
    private static final LocalDate FROM = LocalDate.parse("1996-01-01");
    private static final LocalDate TO   = LocalDate.parse("1996-04-01");

    @Override
    public Set<RelId> usedRelations() {
        return EnumSet.of(
                RelId.SUPPLIER,
                RelId.LINEITEM
        );
    }

    /** Q15 的 root 是 SUPPLIER */
    @Override
    public RelId root() {
        return RelId.SUPPLIER;
    }

    // ============================================================
    // Selection predicate (只对 lineitem 起作用)
    // ============================================================
    @Override
    public boolean passesSelection(Row r) {
        if (r == null) return false;

        switch (r.rel) {

            case LINEITEM: {
                LocalDate ship = r.get("l_shipdate");
                if (ship == null) return false;

                // shipdate ∈ [FROM, TO)
                return !ship.isBefore(FROM) && ship.isBefore(TO);
            }

            case SUPPLIER:
                return true;

            default:
                return false;
        }
    }

    // ============================================================
    // 向下游输出 1 条 incremental revenue 记录
    // ============================================================
    @Override
    public Row buildOutput(Map<RelId, TupleRecord> joined) {

        TupleRecord sup = joined.get(RelId.SUPPLIER);
        TupleRecord li  = joined.get(RelId.LINEITEM);

        if (sup == null || li == null)
            return null;

        // revenue = price * (1 - discount)
        BigDecimal price = li.get("l_extendedprice");
        BigDecimal disc  = li.get("l_discount");
        if (price == null || disc == null) return null;

        BigDecimal revenue = AjuTypes.mul(price, AjuTypes.oneMinus(disc));

        Row out = new Row(RelId.SUPPLIER, OpType.INSERT);
        out.set("s_suppkey", sup.get("s_suppkey"));
        out.set("s_name",    sup.get("s_name"));
        out.set("s_address", sup.get("s_address"));
        out.set("s_phone",   sup.get("s_phone"));
        out.set("revenue",   revenue);

        return out;
    }
}
