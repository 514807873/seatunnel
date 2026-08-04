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

package com.qh.sqlcdc.compare;

import java.math.BigDecimal;

/**
 * 数字主键有序比较（归一化后按 BigDecimal 语义），供双游标归并。
 */
public final class NumericPkComparator {

    private NumericPkComparator() {}

    /**
     * @return &lt;0 源小；0 相等；&gt;0 源大。null 主键视为最小。
     */
    public static int compare(Object[] leftNormalized, Object[] rightNormalized) {
        int len = Math.max(
                leftNormalized == null ? 0 : leftNormalized.length,
                rightNormalized == null ? 0 : rightNormalized.length);
        for (int i = 0; i < len; i++) {
            Object left = leftNormalized != null && i < leftNormalized.length ? leftNormalized[i] : null;
            Object right =
                    rightNormalized != null && i < rightNormalized.length ? rightNormalized[i] : null;
            if (left == null && right == null) {
                continue;
            }
            if (left == null) {
                return -1;
            }
            if (right == null) {
                return 1;
            }
            int cmp = toBigDecimal(left).compareTo(toBigDecimal(right));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static BigDecimal toBigDecimal(Object normalized) {
        if (normalized instanceof BigDecimal) {
            return (BigDecimal) normalized;
        }
        if (normalized instanceof Number) {
            return new BigDecimal(normalized.toString());
        }
        return new BigDecimal(String.valueOf(normalized).trim());
    }
}
