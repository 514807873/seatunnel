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

package org.apache.seatunnel.api.transform;
import org.apache.seatunnel.api.common.GroupConcatQueryResult;
import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.api.source.SeaTunnelJobAware;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.List;

public interface SeaTunnelTransform<T>
        extends Serializable, PluginIdentifierInterface, SeaTunnelJobAware {

    /** call it when Transformer initialed */
    default void open() {}

    /** Inject Flink/Spark subtask index for logging or diagnostics. Default no-op. */
    default void setSubtaskIndex(int subtaskIndex) {}

    /**
     * Set the data type info of input data.
     *
     * @deprecated instead by {@link org.apache.seatunnel.api.table.factory.Factory}
     * @param inputDataType The data type info of upstream input.
     */
    @Deprecated
    default void setTypeInfo(SeaTunnelDataType<T> inputDataType) {
        throw new UnsupportedOperationException("setTypeInfo method is not supported");
    }

    /** Get the catalog table output by this transform */
    CatalogTable getProducedCatalogTable();

    /**
     * Transform input data to {@link this#getProducedCatalogTable().getSeaTunnelRowType()} types
     * data.
     *
     * @param row the data need be transformed.
     * @return transformed data.
     */
    T map(T row);

    default List<T> mapList(T row){
        return null;
    };

    default GroupConcatQueryResult executeGroupConcatQuery(){
        return null;
    };

    /** call it when Transformer completed */
    default void close() {}
}
