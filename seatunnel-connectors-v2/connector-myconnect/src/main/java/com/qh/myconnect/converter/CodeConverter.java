package com.qh.myconnect.converter;

import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.BCUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.SM2;
import cn.hutool.crypto.digest.DigestUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SM4;
import com.qh.myconnect.config.JybSm4Util;
import com.qh.myconnect.config.Util;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Data
public class CodeConverter {
    private Map<String, String> dmMap;
    private AES aes;
    private SM2 sm2;
    private SM4 sm4;
    private JybSm4Util sm4Util;

    public Function<Object, Object> dmConverter(String safeCode) {
        return str -> {
            return dmMap.get(safeCode + '.' + str);
        };
    }

    public CodeConverter() {
        byte[] key = "pangu123pangu123".getBytes();
        this.aes = new AES(key);
        String privateKey = "55d5a83daeacfcebd2c21e61690ae9c30a2fb887793109fe2fd13afade11dfc8";
        String publicKey = "8f7c9b235ce19bc14a94c6affa2592d74e69978123620cb0c06d3edfe87fd37aa0683beb4db1433e218a043a1d0fab670bb758afaae996370b32d68e95b1b805";
        this.sm2 = SmUtil.sm2(BCUtil.toSm2Params(privateKey), BCUtil.toSm2Params(publicKey.substring(0, 64), publicKey.substring(64, 128)));
        this.sm4 = new SM4(key);
    }

    public CodeConverter(String columnName, String encryptKeyId) {
        boolean isUserInterFaceEtL = false;
        List<String> encipherWay = List.of("SM4加密(教育部大数据中心)", "SM4加密", "SM3加密", "SM2加密", "AES加密", "MD5加密", "SM4", "SM3", "SM2", "AES", "MD5");
        Util util = new Util();
        String encipherName = null;
        String publicKey = null;
        String privateKey = null;
        try (Connection con = util.getPanguConnection(); Statement stmt = con.createStatement()) {
            String sql = String.format("SELECT "
                                       + " a.id, "
                                       + " b.encipher_way encipher_way, "
                                       + " b.encipher_name encipher_name, "
                                       + " a.key_content key_content, "
                                       + " a.key_private key_private "
                                       + "FROM "
                                       + " pangu_data_management.secure_custom_key a "
                                       + " LEFT JOIN pangu_data_management.secure_encipher_manage b ON a.encipher_id = b.id  "
                                       + "WHERE "
                                       + " a.id = '%s'", encryptKeyId);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                encipherName = rs.getString("encipher_name");
                publicKey = rs.getString("key_content");
                privateKey = rs.getString("key_private");
            }
            else {
                // 校验是不是用户端etl 根据密钥 id 是不是在 secure_encipher_manage_id里面有值来判断
                String sql2 = String.format("SELECT "
                                            + " a.id,"
                                            + " a.encipher_way "
                                            + "FROM "
                                            + " pangu.seatunnel_table_encrypt a "
                                            + "WHERE "
                                            + " a.key_id = '%s' and columnName='%s'", encryptKeyId,
                        columnName);
                ResultSet rs2 = stmt.executeQuery(sql2);
                if (rs2.next()) {
                    isUserInterFaceEtL = true;
                    encipherName = rs2.getString("encipher_way");
                    if (encipherName.equalsIgnoreCase("SM2")) {
                        privateKey = "55d5a83daeacfcebd2c21e61690ae9c30a2fb887793109fe2fd13afade11dfc8";
                        publicKey = "8f7c9b235ce19bc14a94c6affa2592d74e69978123620cb0c06d3edfe87fd37aa0683beb4db1433e218a043a1d0fab670bb758afaae996370b32d68e95b1b805";
                        new SM2(BCUtil.toSm2Params(privateKey), BCUtil.toSm2Params(publicKey.substring(0, 64),
                                publicKey.substring(64, 128)));
                    }
                    else {
                        String sql3 = String.format("select  app_secret from houyi_catalogue.fdc_campus_apps where "
                                                    + "id='%s'", encryptKeyId);
                        ResultSet rs3 = stmt.executeQuery(sql3);
                        if (rs3.next()) {
                            privateKey = null;
                            publicKey = rs3.getString("app_secret");

                        }
                    }
                }
                else {
                    throw new RuntimeException("秘钥配置有误,请检查");
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (encipherName == null) {
            throw new RuntimeException("未找到加密方式");
        }
        else {
            if (!encipherWay.contains(encipherName)) {
                throw new RuntimeException("不支持加密方式");
            }
        }
        // 教育部提供的sm4加密方式要单独处理,他们的秘钥可以不是16位字符串,他们的是大于16位的字符串,所以需要单独处理
        if (encipherName.equalsIgnoreCase("SM4加密(教育部大数据中心)")) {
            sm4Util = new JybSm4Util(publicKey);
        }
        else if (encipherName.equalsIgnoreCase("SM4加密") || encipherName.equalsIgnoreCase("SM4")) {
            byte[] key = publicKey.substring(0, 16).getBytes();
            this.sm4 = new SM4(key);
        }
        else if (encipherName.equalsIgnoreCase("SM2加密") || encipherName.equalsIgnoreCase("SM2")) {
            this.sm2 = new SM2(BCUtil.toSm2Params(privateKey), BCUtil.toSm2Params(publicKey.substring(0, 64), publicKey.substring(64, 128)));
        }
        else if (encipherName.equalsIgnoreCase("AES加密") || encipherName.equalsIgnoreCase("AES")) {
            assert publicKey != null;
            byte[] key = publicKey.substring(0, 16).getBytes();
            this.aes = new AES(key);
        }
    }

    public Function<Object, Object> encryptConverter(String safeCode) {
        return str -> {
            if (safeCode.startsWith("ENCRYPT.AES")) {
                if (str == null) return null;
                byte[] encrypt = aes.encrypt(String.valueOf(str).getBytes());
                return HexUtil.encodeHexStr(encrypt);
                /**
                 * String decryptedContent = aes.decryptStr(encryptedBytes);
                 */
            }
            if (safeCode.startsWith("ENCRYPT.MD5")) {
                if (str == null) return null;
                return DigestUtil.md5Hex(String.valueOf(str));
            }
            if (safeCode.startsWith("ENCRYPT.SM2")) {
                if (str == null) return null;
                return HexUtil.encodeHexStr(sm2.encrypt(String.valueOf(str).getBytes(StandardCharsets.UTF_8), KeyType.PublicKey));
            }
            if (safeCode.startsWith("ENCRYPT.SM3")) {
                if (str == null) return null;
                return SmUtil.sm3(String.valueOf(str));
            }
            if (safeCode.startsWith("ENCRYPT.SM4")) {
                if (str == null) return null;
                if (sm4Util != null) {
                    try {
                        return sm4Util.encryptEcb(String.valueOf(str));
                    } catch (Exception e) {
                        throw new RuntimeException("加密错误", e);
                    }
                }
                else {
                    return sm4.encryptHex(String.valueOf(str));
                }
            }
            return safeCode + '.' + str;
        };
    }

    public Function<Object, Object> decryptConverter(String safeCode) {
        return str -> {
            if (safeCode.startsWith("DECRYPT.AES")) {
                if (str == null) return null;
                try {
                    return new String(aes.decrypt(HexUtil.decodeHex((String) str)));
                } catch (Exception e) {
                    throw new RuntimeException("解密错误,请检查秘钥配置", e);
                }
            }
            if (safeCode.startsWith("DECRYPT.SM2")) {
                if (str == null) return null;
                try {
                    return new String(sm2.decrypt(HexUtil.decodeHex(String.valueOf(str)), KeyType.PrivateKey),  StandardCharsets.UTF_8);
                } catch (Exception e) {
                    throw new RuntimeException("解密错误,请检查秘钥配置", e);
                }
            }
            if (safeCode.startsWith("DECRYPT.SM4")) {
                if (str == null) return null;
                if (sm4Util != null) {
                    try {
                        return sm4Util.decryptEcb(String.valueOf(str));
                    } catch (Exception e) {
                        throw new RuntimeException("解密错误,请检查秘钥配置", e);
                    }
                }
                else {
                    byte[] decrypt = new byte[0];
                    try {
                        decrypt = sm4.decrypt(String.valueOf(str));
                    } catch (Exception e) {
                        throw new RuntimeException("解密错误,请检查秘钥配置", e);
                    }
                    return new String(decrypt, StandardCharsets.UTF_8);
                }
            }
            return str;
        };
    }

    public static boolean isNumeric(Object obj) {
        if (obj instanceof Number) {
            return true;
        }
        else if (obj instanceof String) {
            try {
                Double.parseDouble((String) obj);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }
}
