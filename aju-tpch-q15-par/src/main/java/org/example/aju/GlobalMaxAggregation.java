package org.example.aju;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * GlobalMaxAggregation:
 *
 * 输入：来自 LocalMaxAggregation 的 LocalMaxRow（每个 worker 的局部最大 supplier）
 * 输出：全局最大 supplier（TopSupplierRow）
 *
 * 实现逻辑：
 *   - 维护全局最大 revenue（maxRevenue）
 *   - 维护达到最大值的 supplier 信息（maxSupplier）
 *   - 每当出现新的全局最大时输出 TopSupplierRow
 */
public class GlobalMaxAggregation
        extends KeyedProcessFunction<Integer,
                                     LocalMaxAggregation.LocalMaxRow,
                                     GlobalMaxAggregation.TopSupplierRow> {

    // =====================================================================
    // 输出结构：最终 Top Supplier（全局最大 revenue）
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

        // ✔ 新增 toString() — 让 print() 输出可读
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
    // 状态：全局最大 revenue 与 supplier 信息
    // =====================================================================
    private transient ValueState<BigDecimal> globalMaxRevenueState;
    private transient ValueState<LocalMaxAggregation.LocalMaxRow> globalMaxSupplierState;

    @Override
    public void open(Configuration parameters) throws Exception {

        globalMaxRevenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "global_max_revenue",
                        BigDecimal.class
                )
        );

        globalMaxSupplierState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "global_max_supplier",
                        TypeInformation.of(new TypeHint<LocalMaxAggregation.LocalMaxRow>() {})
                )
        );
    }

    // =====================================================================
    // 核心：维护全局最大值
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

        // ------------------------------------------------------------
        // Case 1: 新的全局最大 revenue
        // ------------------------------------------------------------
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
