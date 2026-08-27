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

package org.apache.seatunnel.connectors.seatunnel.common.pangu;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.pangu.PanguStore;

/** Local row counters flushed to seatunnel_stream_record on checkpoint complete. */
public final class PanguStreamCounter {

    private long writeCount;
    private long insertCount;
    private long updateCount;
    private long deleteCount;

    public void accept(SeaTunnelRow row) {
        if (row == null || RowKind.UPDATE_BEFORE.equals(row.getRowKind())) {
            return;
        }
        writeCount++;
        RowKind kind = row.getRowKind();
        if (kind == null || RowKind.INSERT.equals(kind)) {
            insertCount++;
        } else if (RowKind.UPDATE_AFTER.equals(kind)) {
            updateCount++;
        } else if (RowKind.DELETE.equals(kind)) {
            deleteCount++;
        } else {
            insertCount++;
        }
    }

    public boolean hasDelta() {
        return writeCount != 0 || insertCount != 0 || updateCount != 0 || deleteCount != 0;
    }

    public void flush(String panguJobId) {
        if (!hasDelta()) {
            return;
        }
        PanguStore.getInstance()
                .addStreamRecord(panguJobId, writeCount, insertCount, updateCount, deleteCount);
        reset();
    }

    public void reset() {
        writeCount = 0;
        insertCount = 0;
        updateCount = 0;
        deleteCount = 0;
    }
}
