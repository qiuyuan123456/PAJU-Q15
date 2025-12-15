package org.example.aju;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.example.aju.AjuTypes.*;

/**
 * Query plan for TPC-H Query 15 (Top Supplier).
 *
 * This class specifies the logical definition of Query 15,
 * including the relations involved, the join structure,
 * the selection predicate, and the construction of incremental
 * output records.
 *
 * Join DAG:
 *   SUPPLIER ← LINEITEM
 *
 * Each valid join result produces an incremental revenue record
 * of the form:
 *   (s_suppkey, s_name, s_address, s_phone, revenue)
 *
 * Aggregation and maximum selection are performed by
 * Q15Aggregation and subsequent aggregation operators.
 */
public class Q15Plan implements QueryPlan {

    /**
     * Time window for the shipping date predicate in Query 15.
     */
    private static final LocalDate FROM = LocalDate.parse("1996-01-01");
    private static final LocalDate TO   = LocalDate.parse("1996-04-01");

    @Override
    public Set<RelId> usedRelations() {
        return EnumSet.of(
                RelId.SUPPLIER,
                RelId.LINEITEM
        );
    }

    /**
     * The root relation of the join DAG.
     */
    @Override
    public RelId root() {
        return RelId.SUPPLIER;
    }

    // ============================================================
    // Selection predicate
    // ============================================================
    @Override
    public boolean passesSelection(Row r) {
        if (r == null) return false;

        switch (r.rel) {

            case LINEITEM: {
                LocalDate ship = r.get("l_shipdate");
                if (ship == null) return false;

                // Shipping date must fall within the specified time window
                return !ship.isBefore(FROM) && ship.isBefore(TO);
            }

            case SUPPLIER:
                return true;

            default:
                return false;
        }
    }

    // ============================================================
    // Construction of incremental output records
    // ============================================================
    @Override
    public Row buildOutput(Map<RelId, TupleRecord> joined) {

        TupleRecord sup = joined.get(RelId.SUPPLIER);
        TupleRecord li  = joined.get(RelId.LINEITEM);

        if (sup == null || li == null)
            return null;

        // Compute revenue contribution for a single line item
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
