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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * 对账归一化：把异构类型收敛成可比较的字面义，供 equals / MD5 指纹共用。
 *
 * <p>规则摘要：null/空白→空；数值含数字串按 BigDecimal 规范；时间固定格式；文本大小写敏感；
 * 二进制保留字节；CLOB→文本；布尔→0/1。
 */
public final class CanonicalNormalizer {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final byte[] NULL_DIGEST_BYTES =
            "\0NULL".getBytes(StandardCharsets.UTF_8);
    private static final byte FIELD_SEP = 0x1F;

    private CanonicalNormalizer() {}

    /**
     * @return null 表示空值；byte[] 表示二进制；String 表示其它规范字面量
     */
    public static Object normalize(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof byte[]) {
                return value;
            }
            if (value instanceof Blob) {
                return blobToBytes((Blob) value);
            }
            if (value instanceof Clob) {
                return normalizeText(clobToString((Clob) value));
            }
            if (value instanceof Boolean) {
                return Boolean.TRUE.equals(value) ? "1" : "0";
            }
            if (value instanceof Number) {
                return normalizeNumber((Number) value);
            }
            if (value instanceof LocalDate) {
                return ((LocalDate) value).format(DATE_FMT);
            }
            if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).format(DATETIME_FMT);
            }
            if (value instanceof LocalTime) {
                return ((LocalTime) value).format(TIME_FMT);
            }
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime().format(DATETIME_FMT);
            }
            if (value instanceof Date) {
                return ((Date) value).toLocalDate().format(DATE_FMT);
            }
            if (value instanceof Time) {
                return ((Time) value).toLocalTime().format(TIME_FMT);
            }
            if (value instanceof java.util.Date) {
                return DATETIME_FMT.format(
                        ((java.util.Date) value)
                                .toInstant()
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime());
            }
            if (value instanceof CharSequence) {
                return normalizeTextOrNumericString(value.toString());
            }
            // 驱动专有类型尽量 toString 后再走文本/数字规则
            return normalizeTextOrNumericString(String.valueOf(value));
        } catch (Exception e) {
            return normalizeTextOrNumericString(String.valueOf(value));
        }
    }

    public static boolean canonicalEquals(Object left, Object right) {
        Object a = normalize(left);
        Object b = normalize(right);
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
        if (a instanceof byte[] || b instanceof byte[]) {
            return false;
        }
        return a.equals(b);
    }

    /**
     * 按 field_mapper 稳定顺序计算行指纹（MD5 hex 小写）。
     *
     * @param valuesBySourceField source 字段名 → 值
     */
    public static String md5Fingerprint(Map<String, Object> valuesBySourceField) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (Map.Entry<String, Object> entry : valuesBySourceField.entrySet()) {
                Object canonical = normalize(entry.getValue());
                updateDigest(md, canonical);
                md.update(FIELD_SEP);
            }
            return toHex(md.digest());
        } catch (Exception e) {
            throw new RuntimeException("计算行指纹失败: " + e.getMessage(), e);
        }
    }

    private static void updateDigest(MessageDigest md, Object canonical) {
        if (canonical == null) {
            md.update(NULL_DIGEST_BYTES);
            return;
        }
        if (canonical instanceof byte[]) {
            md.update((byte[]) canonical);
            return;
        }
        md.update(String.valueOf(canonical).getBytes(StandardCharsets.UTF_8));
    }

    private static String normalizeNumber(Number value) {
        if (value instanceof Float) {
            float f = value.floatValue();
            if (Float.isNaN(f)) {
                return "NaN";
            }
            if (Float.isInfinite(f)) {
                return f > 0 ? "Infinity" : "-Infinity";
            }
            return stripNumber(BigDecimal.valueOf(f));
        }
        if (value instanceof Double) {
            double d = value.doubleValue();
            if (Double.isNaN(d)) {
                return "NaN";
            }
            if (Double.isInfinite(d)) {
                return d > 0 ? "Infinity" : "-Infinity";
            }
            return stripNumber(BigDecimal.valueOf(d));
        }
        if (value instanceof BigDecimal) {
            return stripNumber((BigDecimal) value);
        }
        if (value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte) {
            return Long.toString(value.longValue());
        }
        return stripNumber(new BigDecimal(value.toString()));
    }

    private static String stripNumber(BigDecimal bd) {
        BigDecimal stripped = bd.stripTrailingZeros();
        if (stripped.scale() < 0) {
            stripped = stripped.setScale(0);
        }
        return stripped.toPlainString();
    }

    private static Object normalizeTextOrNumericString(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String bool = normalizeBooleanLiteral(trimmed);
        if (bool != null) {
            return bool;
        }
        if (looksLikeNumber(trimmed)) {
            try {
                return stripNumber(new BigDecimal(trimmed));
            } catch (NumberFormatException ignore) {
                // fall through as text
            }
        }
        return trimmed;
    }

    private static Object normalizeText(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String normalizeBooleanLiteral(String trimmed) {
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if ("true".equals(lower) || "yes".equals(lower)) {
            return "1";
        }
        if ("false".equals(lower) || "no".equals(lower)) {
            return "0";
        }
        return null;
    }

    private static boolean looksLikeNumber(String s) {
        int i = 0;
        if (s.charAt(0) == '+' || s.charAt(0) == '-') {
            if (s.length() == 1) {
                return false;
            }
            i = 1;
        }
        boolean seenDigit = false;
        boolean seenDot = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                seenDigit = true;
            } else if (c == '.' && !seenDot) {
                seenDot = true;
            } else {
                return false;
            }
        }
        return seenDigit;
    }

    private static String clobToString(Clob clob) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Reader reader = clob.getCharacterStream();
                BufferedReader br = new BufferedReader(reader)) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (Exception e) {
            throw new SQLException("读取 CLOB 失败", e);
        }
        return sb.toString();
    }

    private static byte[] blobToBytes(Blob blob) throws SQLException {
        try (InputStream in = blob.getBinaryStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw new SQLException("读取 BLOB 失败", e);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
