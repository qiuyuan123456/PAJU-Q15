package org.example.aju;

import org.apache.flink.api.common.functions.MapFunction;

import static org.example.aju.AjuTypes.*;

/**
 * 解析 supplier.tbl → Row(SUPPLIER)
 *
 * supplier.tbl 格式：
 *   s_suppkey | s_name | s_address | s_nationkey | s_phone | s_acctbal | s_comment | (空)
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
