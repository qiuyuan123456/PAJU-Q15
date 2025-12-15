package org.example.aju;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.example.aju.AjuTypes.*;

/**
 * 解析 lineitem.tbl → Row(LINEITEM)
 *
 * lineitem.tbl 格式（17 列）：
 *   l_orderkey | l_partkey | l_suppkey | l_linenumber | l_quantity |
 *   l_extendedprice | l_discount | l_tax | l_returnflag | l_linestatus |
 *   l_shipdate | l_commitdate | l_receiptdate | l_shipinstruct | l_shipmode |
 *   l_comment | (空)
 */
public class LineitemParseFunction implements MapFunction<String, Row> {

    private static final DateTimeFormatter DF =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public Row map(String line) {

        String[] f = line.split("\\|");

        Row r = new Row(RelId.LINEITEM, OpType.INSERT);

        r.set("l_orderkey",      Integer.parseInt(f[0]));       // FK → ORDERS
        r.set("l_partkey",       Integer.parseInt(f[1]));       // FK → PART
        r.set("l_suppkey",       Integer.parseInt(f[2]));       // FK → SUPPLIER
        r.set("l_linenumber",    Integer.parseInt(f[3]));
        r.set("l_quantity",      new BigDecimal(f[4]));
        r.set("l_extendedprice", new BigDecimal(f[5]));         // Q8 volume 用
        r.set("l_discount",      new BigDecimal(f[6]));         // Q8 volume 用
        r.set("l_tax",           new BigDecimal(f[7]));
        r.set("l_returnflag",    f[8]);
        r.set("l_linestatus",    f[9]);
        r.set("l_shipdate",      LocalDate.parse(f[10], DF));
        r.set("l_commitdate",    LocalDate.parse(f[11], DF));
        r.set("l_receiptdate",   LocalDate.parse(f[12], DF));
        r.set("l_shipinstruct",  f[13]);
        r.set("l_shipmode",      f[14]);
        r.set("l_comment",       f[15]);

        return r;
    }
}
