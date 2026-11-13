package org.apache.seatunnel.connectors.seatunnel.groovy.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * 用户用 Groovy 实现的取数脚本。
 * open 只调用一次，结果放在脚本自己的字段里，pollNext 接着读。
 * pollNext 的参数可以不写类型。
 * output.collect 传 Map，key 就是字段名，顺序要和 schema.fields 一致。
 */
public interface GroovySourceScript {

    /**
     * 初始化。适合先请求第一个接口，把结果留在脚本自己的字段里。
     */
    void open() throws Exception;

    /**
     * 拉下一批数据。通过 output.collect 发给下游。
     *
     * @return false 表示没有更多数据
     */
    boolean pollNext(Collector<SeaTunnelRow> output) throws Exception;
}
