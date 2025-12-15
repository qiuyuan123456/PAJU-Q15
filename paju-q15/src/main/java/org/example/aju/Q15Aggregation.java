package org.example.aju;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

import static org.example.aju.AjuTypes.*;

/**
 * Q15Aggregation.
 *
 * This operator incrementally maintains the total revenue for each supplier
 * based on the output of Q15Plan. Each input record represents an incremental
 * revenue contribution associated with a supplier.
 *
 * The operator aggregates revenue per supplier and emits updated cumulative
 * results after each incremental change.
 */
public class Q15Aggregation
        extends KeyedProcessFunction<Integer, Row, Q15Aggregation.Q15AggRow> {

    // ===================================================================
    // Output record: aggregated revenue per supplier
    // ===================================================================
    public static class Q15AggRow {
        public int sSuppkey;
        public String sName;
        public String sAddress;
        public String sPhone;
        public BigDecimal totalRevenue;

        public Q15AggRow(int k, String n, String addr, String phone, BigDecimal rev) {
            this.sSuppkey = k;
            this.sName = n;
            this.sAddress = addr;
            this.sPhone = phone;
            this.totalRevenue = rev;
        }

        @Override
        public String toString() {
            return String.format(
                "Q15AggRow(s_suppkey=%d, name=%s, revenue=%s)",
                sSuppkey, sName, totalRevenue.toPlainString()
            );
        }
    }

    // ===================================================================
    // State: static supplier information and cumulative revenue
    // ===================================================================
    private transient ValueState<Tuple3<String, String, String>> infoState;
    private transient ValueState<BigDecimal> revenueState;

    @Override
    public void open(Configuration parameters) {

        // State for storing static supplier attributes
        infoState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "supplier_info",
                        TypeInformation.of(
                                new TypeHint<Tuple3<String, String, String>>() {}
                        )
                )
        );

        // State for maintaining cumulative revenue per supplier
        revenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "supplier_revenue",
                        BigDecimal.class
                )
        );
    }

    // ===================================================================
    // Incremental revenue aggregation
    // ===================================================================
    @Override
    public void processElement(Row r,
                               Context ctx,
                               Collector<Q15AggRow> out) throws Exception {

        int suppkey = r.get("s_suppkey");
        String name = r.get("s_name");
        String addr = r.get("s_address");
        String phone = r.get("s_phone");
        BigDecimal delta = r.get("revenue");

        // Initialize static supplier information if not yet present
        Tuple3<String, String, String> info = infoState.value();
        if (info == null) {
            info = Tuple3.of(name, addr, phone);
            infoState.update(info);
        }

        // Update cumulative revenue using incremental delta
        BigDecimal cur = revenueState.value();
        if (cur == null) cur = BigDecimal.ZERO;

        if (r.op == OpType.INSERT)
            cur = cur.add(delta);
        else
            cur = cur.subtract(delta);

        revenueState.update(cur);

        // Emit updated aggregated result
        out.collect(new Q15AggRow(
                suppkey,
                info.f0,  // supplier name
                info.f1,  // supplier address
                info.f2,  // supplier phone
                cur       // cumulative revenue
        ));
    }
}
