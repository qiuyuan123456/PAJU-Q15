package org.example.aju;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.EnumMap;
import java.util.Map;

import static org.example.aju.AjuTypes.*;

/**
 * TPC-H Query 15 (Top Supplier) — 并行版
 *
 * 支持：
 *   - Join 阶段按 supplier 分区并行
 *   - 增量 SUM(revenue)
 *   - 两层 Max 聚合（LocalMax → GlobalMax）
 */
public class TpchAjuJobQ15 {

    private static void checkFile(String path) {
        File f = new File(path);
        if (!f.exists())
            throw new RuntimeException("FATAL: missing TPC-H data file: " + path);
    }

    public static void main(String[] args) throws Exception {

        // =====================================================================
        // Flink 环境
        // =====================================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);   // ← 这里你可以改成 1,4,8,12 来做论文式实验

        // =====================================================================
        // 构建 Q15 join schema DAG
        // =====================================================================
        Map<RelId, RelationMeta> schema = new EnumMap<>(RelId.class);

        RelationMeta supplier =
                new RelationMeta(RelId.SUPPLIER, "s_suppkey");

        RelationMeta lineitem =
                new RelationMeta(RelId.LINEITEM, "l_orderkey")
                        .addParent(RelId.SUPPLIER, "l_suppkey");

        supplier.addChild(RelId.LINEITEM);

        schema.put(RelId.SUPPLIER, supplier);
        schema.put(RelId.LINEITEM, lineitem);

        QueryPlan plan = new Q15Plan();

        // =====================================================================
        // TPC-H 数据路径
        // =====================================================================
        String base = "/home/qiuyuan/tpch-data02";

        checkFile(base + "/supplier.tbl");
        checkFile(base + "/lineitem.tbl");

        // =====================================================================
        // 数据加载
        // =====================================================================
        DataStream<Row> supplierSrc =
                env.readTextFile(base + "/supplier.tbl")
                   .map(new SupplierParseFunction());

        DataStream<Row> lineitemSrc =
                env.readTextFile(base + "/lineitem.tbl")
                   .map(new LineitemParseFunction());

        // =====================================================================
        // 并行 AJU Join 阶段（关键修改）
        // 使用 AcyclicJoinKeySelector 按 join key 分区
        // =====================================================================
        SingleOutputStreamOperator<Row> deltas =
                supplierSrc.union(lineitemSrc)
                        .keyBy(new AcyclicJoinKeySelector())   // ← 替代 keyBy(0)
                        .process(new AcyclicJoinProcessFunction(schema, plan));

        // =====================================================================
        // 增量 SUM(revenue)
        // 这里已经是按 s_suppkey 分区，所以天然并行
        // =====================================================================
        SingleOutputStreamOperator<Q15Aggregation.Q15AggRow> aggregated =
                deltas
                        .keyBy(r -> (Integer) r.get("s_suppkey"))
                        .process(new Q15Aggregation());

        // =====================================================================
        // 两层 Max 聚合（论文中的分布式 top-k 结构）
        // Step 1: LocalMaxAggregation（每个 worker 维护局部最大 revenue）
        // =====================================================================
        SingleOutputStreamOperator<LocalMaxAggregation.LocalMaxRow> localMax =
                aggregated
                        .keyBy(r -> 0)   // 每个 worker 内部局部最大（不需要按 key 分 supplier）
                        .process(new LocalMaxAggregation());

        // =====================================================================
        // Step 2: GlobalMaxAggregation（合并所有 worker 的 local max）
        // =====================================================================
        SingleOutputStreamOperator<GlobalMaxAggregation.TopSupplierRow> top =
                localMax
                        .keyBy(r -> 0)  // 全局合并
                        .process(new GlobalMaxAggregation());

        // =====================================================================
        // 输出结果
        // =====================================================================
        top.print();

        env.execute("TPC-H Q15 (Top Supplier) — Parallel AJU Version");
    }
}
