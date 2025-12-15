package org.example.aju;

import org.apache.flink.api.java.functions.KeySelector;
import static org.example.aju.AjuTypes.*;

/**
 * KeySelector used to enable parallel execution of the
 * AcyclicJoinProcessFunction.
 *
 * Tuples are partitioned according to the join key:
 *   - SUPPLIER tuples are keyed by s_suppkey
 *   - LINEITEM tuples are keyed by l_suppkey
 *
 * This partitioning strategy ensures that all tuples belonging to the
 * same supplier are routed to the same partition, so that each supplier’s
 * join subtree is processed by a single worker, while different suppliers
 * can be processed in parallel across multiple workers.
 */
public class AcyclicJoinKeySelector implements KeySelector<Row, Integer> {

    @Override
    public Integer getKey(Row r) {

        switch (r.rel) {

            case SUPPLIER:
                return (Integer) r.get("s_suppkey");

            case LINEITEM:
                return (Integer) r.get("l_suppkey");

            default:
                return 0;
        }
    }
}
