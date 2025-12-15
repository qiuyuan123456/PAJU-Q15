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

/**
 * LocalMaxAggregation.
 *
 * This operator maintains the local maximum revenue within each worker.
 * It receives aggregated revenue records for individual suppliers and
 * identifies the supplier with the highest total revenue on each worker.
 *
 * The local maximum results are subsequently forwarded to
 * GlobalMaxAggregation for global consolidation.
 */
public class LocalMaxAggregation extends KeyedProcessFunction<Integer,
        Q15Aggregation.Q15AggRow,
        LocalMaxAggregation.LocalMaxRow> {

    // =====================================================================
    // Output record: local top supplier per worker
    // =====================================================================
    public static class LocalMaxRow {
        public int sSuppkey;
        public String sName;
        public String sAddress;
        public String sPhone;
        public BigDecimal revenue;

        public LocalMaxRow(int k, String n, String a, String p, BigDecimal r) {
            this.sSuppkey = k;
            this.sName = n;
            this.sAddress = a;
            this.sPhone = p;
            this.revenue = r;
        }
    }

    // =====================================================================
    // State: local maximum revenue and corresponding supplier
    // =====================================================================
    private transient ValueState<BigDecimal> localMaxRevState;
    private transient ValueState<Q15Aggregation.Q15AggRow> localMaxSupplierState;

    @Override
    public void open(Configuration parameters) throws Exception {

        // State for tracking the current local maximum revenue
        localMaxRevState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "local_max_revenue",
                        BigDecimal.class
                )
        );

        // State for storing the supplier associated with the local maximum
        localMaxSupplierState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "local_max_supplier",
                        TypeInformation.of(
                                new TypeHint<Q15Aggregation.Q15AggRow>() {}
                        )
                )
        );
    }

    // =====================================================================
    // Local maximum maintenance
    // =====================================================================
    @Override
    public void processElement(
            Q15Aggregation.Q15AggRow r,
            Context ctx,
            Collector<LocalMaxRow> out) throws Exception {

        BigDecimal rev = r.totalRevenue;

        BigDecimal localMax = localMaxRevState.value();
        if (localMax == null)
            localMax = BigDecimal.valueOf(-1);

        // Update the local maximum if a larger revenue value is observed
        if (rev.compareTo(localMax) > 0) {

            localMaxRevState.update(rev);
            localMaxSupplierState.update(r);

            out.collect(new LocalMaxRow(
                    r.sSuppkey,
                    r.sName,
                    r.sAddress,
                    r.sPhone,
                    rev
            ));
        }
    }
}
