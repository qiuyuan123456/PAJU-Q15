package org.example.aju;

import org.apache.flink.api.java.functions.KeySelector;
import static org.example.aju.AjuTypes.*;

/**
 * 用于让 AcyclicJoinProcessFunction 支持多 worker 并行的 KeySelector。
 *
 * 将 tuple 按 join key 进行分区：
 *   - SUPPLIER 按 s_suppkey
 *   - LINEITEM 按 l_suppkey
 *
 * 这样每个 supplier 的 join 子树会落入同一个分区，
 * 不同 supplier 可以被分到不同 worker，实现真正并行 join。
 */
public class AcyclicJoinKeySelector implements KeySelector<Row, Integer> {

    @Override
    public Integer getKey(Row r) {

        switch (r.rel) {

            case SUPPLIER:
                return (Integer) r.get("s_suppkey");

            case LINEITEM:
                return (Integer) r.get("l_suppkey");

            default:
                // 理论上 Q15 不应该出现别的关系
                return 0;
        }
    }
}
