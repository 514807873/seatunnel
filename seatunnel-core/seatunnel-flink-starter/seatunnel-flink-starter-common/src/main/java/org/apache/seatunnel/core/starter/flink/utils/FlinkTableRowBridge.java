/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.utils;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/** Bridge between SeaTunnelRow DataStream and Flink Table Row DataStream. */
public final class FlinkTableRowBridge {

    private FlinkTableRowBridge() {}

    public static DataStream<Row> toFlinkRowStream(
            DataStream<SeaTunnelRow> stream, SeaTunnelRowType rowType) {
        @SuppressWarnings("unchecked")
        TypeInformation<Row> typeInfo = (TypeInformation<Row>) TypeConverterUtils.convert(rowType);
        return stream.map(
                        (MapFunction<SeaTunnelRow, Row>)
                                seaTunnelRow -> {
                                    int arity = seaTunnelRow.getArity();
                                    Row row = new Row(arity);
                                    for (int i = 0; i < arity; i++) {
                                        row.setField(i, seaTunnelRow.getField(i));
                                    }
                                    return row;
                                })
                .returns(typeInfo);
    }

    public static DataStream<SeaTunnelRow> toSeaTunnelRowStream(DataStream<Row> stream) {
        return stream.map(
                        (MapFunction<Row, SeaTunnelRow>)
                                row -> {
                                    Object[] fields = new Object[row.getArity()];
                                    for (int i = 0; i < row.getArity(); i++) {
                                        fields[i] = row.getField(i);
                                    }
                                    SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
                                    seaTunnelRow.setRowKind(RowKind.INSERT);
                                    return seaTunnelRow;
                                })
                .returns(TypeInformation.of(SeaTunnelRow.class));
    }
}
