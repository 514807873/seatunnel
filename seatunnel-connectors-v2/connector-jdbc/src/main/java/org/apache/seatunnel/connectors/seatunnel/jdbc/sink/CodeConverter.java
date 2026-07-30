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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import cn.hutool.core.util.HexUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.SM2;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SM4;
import lombok.Data;

import java.util.Map;

@Data
class CodeConverter {
    private Map<String, String> dmMap;
    private AES aes;
    private SM2 sm2;
    private SM4 sm4;

    CodeConverter() {
        byte[] key = "pangu123pangu123".getBytes();
        this.aes = new AES(key);
        byte[] privateKey =
                HexUtil.decodeHex(
                        "308193020100301306072a8648ce3d020106082a811ccf5501822d0479307702010104203192b1f7b849bcaef11e682b09d4d719f30b5ba43f2be6f81ac289ee2e50f9b8a00a06082a811ccf5501822da144034200041122423fd69fb39e8cb09d0269cdda139513f22c080eacda9158047ac8c6f3bd1193c01fa81dd3896c01ac9a554c4d9feacb9a80677bc493363c8b9e83f42f99");
        byte[] publicKey =
                HexUtil.decodeHex(
                        "3059301306072a8648ce3d020106082a811ccf5501822d034200041122423fd69fb39e8cb09d0269cdda139513f22c080eacda9158047ac8c6f3bd1193c01fa81dd3896c01ac9a554c4d9feacb9a80677bc493363c8b9e83f42f99");
        this.sm2 = SmUtil.sm2(privateKey, publicKey);
        this.sm4 = new SM4(key);
    }

    String convert(String safeCode, String str) {
        if (safeCode == null) {
            return str;
        }
        if (safeCode.startsWith("ENCRYPT")) {
            return encryptConverter(safeCode, str);
        }
        return dmConverter(safeCode, str);
    }

    private String dmConverter(String safeCode, String str) {
        if (dmMap == null) {
            return str;
        }
        return dmMap.get(safeCode + '.' + str);
    }

    private String encryptConverter(String safeCode, String str) {
        if (safeCode.equalsIgnoreCase("ENCRYPT.AES")) {
            byte[] encrypt = aes.encrypt(String.valueOf(str).getBytes());
            return HexUtil.encodeHexStr(encrypt);
        } else if (safeCode.equalsIgnoreCase("ENCRYPT.MD5")) {
            return DigestUtil.md5Hex(String.valueOf(str));
        } else if (safeCode.equalsIgnoreCase("ENCRYPT.SM2")) {
            return sm2.encryptBcd(String.valueOf(str), KeyType.PublicKey);
        } else if (safeCode.equalsIgnoreCase("ENCRYPT.SM3")) {
            return SmUtil.sm3(String.valueOf(str));
        } else if (safeCode.equalsIgnoreCase("ENCRYPT.SM4")) {
            return sm4.encryptHex(String.valueOf(str));
        }
        return str;
    }
}
