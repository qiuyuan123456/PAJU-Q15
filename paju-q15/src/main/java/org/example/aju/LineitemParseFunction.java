package org.example.aju;

import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.example.aju.AjuTypes.*;

/**
 * Parses records from lineitem.tbl into Row(LINEITEM).
 *
 * The lineitem.tbl file contains 17 fields in the following order:
 *   l_orderkey | l_partkey | l_suppkey | l_linenumber | l_quantity |
 *   l_extendedprice | l_discount | l_tax | l_returnflag | l_linestatus |
 *   l_shipdate | l_commitdate | l_receiptdate | l_shipinstruct | l_shipmode |
 *   l_comment | (empty)
 */
public class LineitemParseFunction implements MapFunction<String, Row> {

    private static final DateTimeFormatter DF =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public Row map(String line) {

        String[] f = line.split("\\|");

        Row r = new Row(RelId.LINEITEM, OpType.INSERT);

        r.set("l_orderkey",      Integer.parseInt(f[0]));       // Foreign key to ORDERS
        r.set("l_partkey",       Integer.parseInt(f[1]));       // Foreign key to PART
        r.set("l_suppkey",       Integer.parseInt(f[2]));       // Foreign key to SUPPLIER
        r.set("l_linenumber",    Integer.parseInt(f[3]));
        r.set("l_quantity",      new BigDecimal(f[4]));
        r.set("l_extendedprice", new BigDecimal(f[5]));         // Used for revenue computation
        r.set("l_discount",      new BigDecimal(f[6]));         // Used for revenue computation
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
