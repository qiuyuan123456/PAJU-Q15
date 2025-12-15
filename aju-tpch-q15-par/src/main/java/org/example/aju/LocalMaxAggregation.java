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
 * LocalMaxAggregation:
 *
 * 在每个 worker 内部维护该 worker 的局部最大 revenue。
 * 输入是 Q15Aggregation.Q15AggRow（每个 supplier 的 totalRevenue）。
 *
 * 输出 LocalMaxRow：
 *   - worker 内部最大 revenue
 *   - 以及对应的 supplier 信息
 *
 * GlobalMaxAggregation 会将所有 worker 的输出再做全局最大值合并。
 */
public class LocalMaxAggregation extends KeyedProcessFunction<Integer,
        Q15Aggregation.Q15AggRow,
        LocalMaxAggregation.LocalMaxRow> {

    // =====================================================================
    // 输出结构：每个 worker 的局部最大 supplier
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
    // 状态：记录当前 worker 的最大 revenue 与对应 supplier 信息
    // =====================================================================
    private transient ValueState<BigDecimal> localMaxRevState;
    private transient ValueState<Q15Aggregation.Q15AggRow> localMaxSupplierState;

    @Override
    public void open(Configuration parameters) throws Exception {

        localMaxRevState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "local_max_revenue",
                        BigDecimal.class
                )
        );

        localMaxSupplierState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "local_max_supplier",
                        TypeInformation.of(new TypeHint<Q15Aggregation.Q15AggRow>() {})
                )
        );
    }

    // =====================================================================
    // 核心逻辑：维护局部最大 revenue
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

        // ------------------------------------------------------------
        // 如果出现新的局部最大值，则替换并输出
        // ------------------------------------------------------------
        if (rev.compareTo(localMax) > 0) {

            // 更新状态
            localMaxRevState.update(rev);
            localMaxSupplierState.update(r);

            // 输出局部最大 supplier 信息
            out.collect(new LocalMaxRow(
                    r.sSuppkey,
                    r.sName,
                    r.sAddress,
                    r.sPhone,
                    rev
            ));
        }

        // （注意：不处理 rev == localMax 的情况，
        //  因为 GlobalMaxAggregation 会负责合并同分值情况；
        //  局部层目标是“找到最大值”，不是保留所有并列。）
    }
}
