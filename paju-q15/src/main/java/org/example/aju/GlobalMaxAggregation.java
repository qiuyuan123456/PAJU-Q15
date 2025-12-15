package org.example.aju;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * GlobalMaxAggregation.
 *
 * This operator performs the global aggregation step for TPC-H Query 15.
 * It receives local maximum suppliers produced by each worker and
 * maintains the global maximum revenue across all workers.
 *
 * The operator outputs a TopSupplierRow whenever a new global maximum
 * revenue is observed.
 */
public class GlobalMaxAggregation
        extends KeyedProcessFunction<Integer,
                                     LocalMaxAggregation.LocalMaxRow,
                                     GlobalMaxAggregation.TopSupplierRow> {

    // =====================================================================
    // Output record: final top supplier with global maximum revenue
    // =====================================================================
    public static class TopSupplierRow {
        public int sSuppkey;
        public String sName;
        public String sAddress;
        public String sPhone;
        public BigDecimal revenue;

        public TopSupplierRow(int k, String n, String a, String p, BigDecimal r) {
            this.sSuppkey = k;
            this.sName = n;
            this.sAddress = a;
            this.sPhone = p;
            this.revenue = r;
        }

        /**
         * String representation used for readable output.
         */
        @Override
        public String toString() {
            return "TopSupplierRow{" +
                    "sSuppkey=" + sSuppkey +
                    ", sName='" + sName + '\'' +
                    ", sAddress='" + sAddress + '\'' +
                    ", sPhone='" + sPhone + '\'' +
                    ", revenue=" + revenue +
                    '}';
        }
    }

    // =====================================================================
    // State: global maximum revenue and corresponding supplier
    // =====================================================================
    private transient ValueState<BigDecimal> globalMaxRevenueState;
    private transient ValueState<LocalMaxAggregation.LocalMaxRow> globalMaxSupplierState;

    @Override
    public void open(Configuration parameters) throws Exception {

        // State for tracking the current global maximum revenue
        globalMaxRevenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "global_max_revenue",
                        BigDecimal.class
                )
        );

        // State for storing the supplier associated with the global maximum
        globalMaxSupplierState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "global_max_supplier",
                        TypeInformation.of(
                                new TypeHint<LocalMaxAggregation.LocalMaxRow>() {}
                        )
                )
        );
    }

    // =====================================================================
    // Global maximum maintenance
    // =====================================================================
    @Override
    public void processElement(
            LocalMaxAggregation.LocalMaxRow r,
            Context ctx,
            Collector<TopSupplierRow> out) throws Exception {

        BigDecimal rev = r.revenue;

        BigDecimal globalMax = globalMaxRevenueState.value();
        if (globalMax == null)
            globalMax = BigDecimal.valueOf(-1);

        // Update the global maximum if a larger value is observed
        if (rev.compareTo(globalMax) > 0) {

            globalMaxRevenueState.update(rev);
            globalMaxSupplierState.update(r);

            out.collect(new TopSupplierRow(
                    r.sSuppkey,
                    r.sName,
                    r.sAddress,
                    r.sPhone,
                    rev
            ));
        }
    }
}
