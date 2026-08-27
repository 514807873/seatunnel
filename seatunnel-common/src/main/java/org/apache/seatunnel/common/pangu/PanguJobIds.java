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

package org.apache.seatunnel.common.pangu;

/**
 * Resolve Pangu interface jobId for seatunnel_jobs_offset / seatunnel_stream_record.
 *
 * <p>Prefer job env {@code pangu-job-id}, fall back to {@code seaTunnelJobId} for Flink leftover.
 */
public final class PanguJobIds {

    public static final String ENV_KEY = "pangu-job-id";
    public static final String LEGACY_ENV = "seaTunnelJobId";

    private PanguJobIds() {}

    public static String resolve(String panguJobIdFromContext) {
        if (panguJobIdFromContext != null && !panguJobIdFromContext.isEmpty()) {
            return panguJobIdFromContext;
        }
        String legacy = System.getenv(LEGACY_ENV);
        if (legacy != null && !legacy.isEmpty()) {
            return legacy;
        }
        return null;
    }
}
