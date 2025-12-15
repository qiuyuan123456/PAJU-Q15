package org.example.aju;

import org.apache.flink.api.common.functions.MapFunction;

import static org.example.aju.AjuTypes.*;

/**
 * Parser for the SUPPLIER relation.
 *
 * This function parses raw text lines from the TPC-H
 * supplier.tbl file and converts each line into a
 * Row object representing a SUPPLIER tuple.
 *
 * Input format of supplier.tbl:
 *   s_suppkey | s_name | s_address | s_nationkey |
 *   s_phone | s_acctbal | s_comment | (empty)
 *
 * Each parsed tuple is emitted as an INSERT operation
 * and serves as static input data for the incremental
 * join and aggregation pipeline.
 */
public class SupplierParseFunction implements MapFunction<String, Row> {

    @Override
    public Row map(String line) {

        String[] f = line.split("\\|");

        Row r = new Row(RelId.SUPPLIER, OpType.INSERT);

        r.set("s_suppkey",   Integer.parseInt(f[0]));
        r.set("s_name",      f[1]);
        r.set("s_address",   f[2]);
        r.set("s_nationkey", Integer.parseInt(f[3]));
        r.set("s_phone",     f[4]);
        r.set("s_acctbal",   new java.math.BigDecimal(f[5]));
        r.set("s_comment",   f[6]);

        return r;
    }
}
