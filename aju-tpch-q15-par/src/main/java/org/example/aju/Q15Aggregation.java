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
 * Q15Aggregation:
 *
 * 接受 Q15Plan 输出的一行：
 *   (s_suppkey, s_name, s_address, s_phone, revenue)
 *
 * 并维护：每个 supplier 的累计 total_revenue
 *
 * 输出：
 *   Q15AggRow(s_suppkey, s_name, s_address, s_phone, total_revenue)
 */
public class Q15Aggregation
        extends KeyedProcessFunction<Integer, Row, Q15Aggregation.Q15AggRow> {

    // ===================================================================
    // 输出结构（聚合后的结果）
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
    // 状态（每个 supplier 的 static info + 累计 revenue）
    // ===================================================================
    private transient ValueState<Tuple3<String, String, String>> infoState;
    private transient ValueState<BigDecimal> revenueState;

    @Override
    public void open(Configuration parameters) {

        infoState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "supplier_info",
                        TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {})
                )
        );

        revenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                        "supplier_revenue",
                        BigDecimal.class
                )
        );
    }

    // ===================================================================
    // 每来一条 incremental revenue，更新 supplier 的 total revenue
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

        // ----------- 更新 static info -----------
        Tuple3<String, String, String> info = infoState.value();
        if (info == null) {
            info = Tuple3.of(name, addr, phone);
            infoState.update(info);
        }

        // ----------- 更新 total revenue -----------
        BigDecimal cur = revenueState.value();
        if (cur == null) cur = BigDecimal.ZERO;

        if (r.op == OpType.INSERT)
            cur = cur.add(delta);
        else
            cur = cur.subtract(delta);

        revenueState.update(cur);

        // ----------- 输出累积值 -----------
        out.collect(new Q15AggRow(
                suppkey,
                info.f0,  // name
                info.f1,  // address
                info.f2,  // phone
                cur       // total revenue
        ));
    }
}
