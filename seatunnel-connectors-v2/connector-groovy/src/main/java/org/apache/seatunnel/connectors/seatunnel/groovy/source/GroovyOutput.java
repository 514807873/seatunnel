package org.apache.seatunnel.connectors.seatunnel.groovy.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 脚本里的 output。collect 接受 Map，按 schema 字段顺序装成一行。
 */
final class GroovyOutput {

    private final Collector<SeaTunnelRow> delegate;
    private final List<String> fieldNames;

    GroovyOutput(Collector<SeaTunnelRow> delegate, List<String> fieldNames) {
        this.delegate = delegate;
        this.fieldNames = fieldNames == null ? new ArrayList<>() : fieldNames;
    }

    public void collect(Object record) {
        if (record instanceof SeaTunnelRow) {
            delegate.collect((SeaTunnelRow) record);
            return;
        }
        if (record instanceof Map) {
            delegate.collect(toRow((Map<?, ?>) record));
            return;
        }
        throw new SeaTunnelException("collect expects Map or SeaTunnelRow");
    }

    public Object getCheckpointLock() {
        return delegate.getCheckpointLock();
    }

    private SeaTunnelRow toRow(Map<?, ?> map) {
        List<String> names = fieldNames;
        if (names.isEmpty()) {
            names = new ArrayList<>();
            for (Object key : map.keySet()) {
                if (key != null) {
                    names.add(String.valueOf(key));
                }
            }
        }
        Object[] values = new Object[names.size()];
        for (int i = 0; i < names.size(); i++) {
            values[i] = valueOf(map, names.get(i));
        }
        return new SeaTunnelRow(values);
    }

    private static Object valueOf(Map<?, ?> map, String fieldName) {
        if (map.containsKey(fieldName)) {
            return map.get(fieldName);
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() != null && fieldName.equals(String.valueOf(entry.getKey()))) {
                return entry.getValue();
            }
        }
        return null;
    }
}
