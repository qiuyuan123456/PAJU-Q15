package org.example.aju;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.EnumMap;
import java.util.Map;

import static org.example.aju.AjuTypes.*;

/**
 * Parallel implementation of TPC-H Query 15 (Top Supplier).
 *
 * This job implements an incremental query processing pipeline
 * based on the AJU model, supporting:
 *   - Key-partitioned parallel join on supplier identifiers
 *   - Incremental revenue aggregation per supplier
 *   - Two-level distributed maximum computation
 *     (LocalMax followed by GlobalMax)
 *
 * The job is designed to evaluate scalability with respect to
 * both data size and degree of parallelism.
 */
public class TpchAjuJobQ15 {

    /**
     * Checks whether the required TPC-H data file exists.
     */
    private static void checkFile(String path) {
        File f = new File(path);
        if (!f.exists())
            throw new RuntimeException("FATAL: missing TPC-H data file: " + path);
    }

    public static void main(String[] args) throws Exception {

        // =====================================================================
        // Flink execution environment
        // =====================================================================
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Degree of parallelism; varied in experiments
        env.setParallelism(4);

        // =====================================================================
        // Construction of the Q15 join schema (acyclic join DAG)
        // =====================================================================
        Map<RelId, RelationMeta> schema = new EnumMap<>(RelId.class);

        // SUPPLIER is the root relation
        RelationMeta supplier =
                new RelationMeta(RelId.SUPPLIER, "s_suppkey");

        // LINEITEM references SUPPLIER via foreign key l_suppkey
        RelationMeta lineitem =
                new RelationMeta(RelId.LINEITEM, "l_orderkey")
                        .addParent(RelId.SUPPLIER, "l_suppkey");

        supplier.addChild(RelId.LINEITEM);

        schema.put(RelId.SUPPLIER, supplier);
        schema.put(RelId.LINEITEM, lineitem);

        // Query-specific logic for TPC-H Q15
        QueryPlan plan = new Q15Plan();

        // =====================================================================
        // TPC-H data paths
        // =====================================================================
        String base = "/home/qiuyuan/tpch-data02";

        checkFile(base + "/supplier.tbl");
        checkFile(base + "/lineitem.tbl");

        // =====================================================================
        // Data ingestion and parsing
        // =====================================================================
        DataStream<Row> supplierSrc =
                env.readTextFile(base + "/supplier.tbl")
                   .map(new SupplierParseFunction());

        DataStream<Row> lineitemSrc =
                env.readTextFile(base + "/lineitem.tbl")
                   .map(new LineitemParseFunction());

        // =====================================================================
        // Parallel AJU join stage
        // Tuples are partitioned by supplier key so that all tuples
        // belonging to the same supplier are processed by the same worker
        // =====================================================================
        SingleOutputStreamOperator<Row> deltas =
                supplierSrc.union(lineitemSrc)
                        .keyBy(new AcyclicJoinKeySelector())
                        .process(new AcyclicJoinProcessFunction(schema, plan));

        // =====================================================================
        // Incremental aggregation: SUM(revenue) per supplier
        // This stage is naturally parallel due to key partitioning
        // =====================================================================
        SingleOutputStreamOperator<Q15Aggregation.Q15AggRow> aggregated =
                deltas
                        .keyBy(r -> (Integer) r.get("s_suppkey"))
                        .process(new Q15Aggregation());

        // =====================================================================
        // Distributed maximum computation
        // Step 1: Local maximum per worker
        // =====================================================================
        SingleOutputStreamOperator<LocalMaxAggregation.LocalMaxRow> localMax =
                aggregated
                        .keyBy(r -> 0)
                        .process(new LocalMaxAggregation());

        // =====================================================================
        // Step 2: Global maximum across all workers
        // =====================================================================
        SingleOutputStreamOperator<GlobalMaxAggregation.TopSupplierRow> top =
                localMax
                        .keyBy(r -> 0)
                        .process(new GlobalMaxAggregation());

        // =====================================================================
        // Result output
        // =====================================================================
        top.print();

        env.execute("TPC-H Q15 (Top Supplier) — Parallel AJU Version");
    }
}
