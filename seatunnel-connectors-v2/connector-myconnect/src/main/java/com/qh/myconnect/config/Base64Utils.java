package com.qh.myconnect.config;

import java.util.Base64;
import java.util.regex.Pattern;

/**
 * Base64验证工具类
 */
public class Base64Utils {

    // 标准Base64正则表达式
    private static final Pattern BASE64_PATTERN = Pattern.compile(
        "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$"
    );
    
    // URL安全的Base64正则表达式
    private static final Pattern BASE64_URL_PATTERN = Pattern.compile(
        "^([A-Za-z0-9-_]{4})*([A-Za-z0-9-_]{3}=|[A-Za-z0-9-_]{2}==)?$"
    );

    /**
     * 验证字符串是否是有效的Base64编码
     * @param str 要验证的字符串
     * @return 如果是有效的Base64编码返回true，否则返回false
     */
    public static boolean isBase64(String str) {
        // 空值检查
        if (str == null || str.isEmpty()) {
            return false;
        }
        
        // 检查长度是否为4的倍数
        if (str.length() % 4 != 0) {
            return false;
        }
        
        // 检查是否符合Base64或URL安全的Base64格式
        boolean isStandardBase64 = BASE64_PATTERN.matcher(str).matches();
        boolean isUrlSafeBase64 = BASE64_URL_PATTERN.matcher(str).matches();
        
        if (!isStandardBase64 && !isUrlSafeBase64) {
            return false;
        }
        
        // 尝试解码验证
        try {
            Base64.getDecoder().decode(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * 验证字符串是否是有效的Base64编码（仅标准Base64）
     * @param str 要验证的字符串
     * @return 如果是有效的标准Base64编码返回true，否则返回false
     */
    public static boolean isStandardBase64(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        
        if (str.length() % 4 != 0) {
            return false;
        }
        
        if (!BASE64_PATTERN.matcher(str).matches()) {
            return false;
        }
        
        try {
            Base64.getDecoder().decode(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * 验证字符串是否是有效的URL安全的Base64编码
     * @param str 要验证的字符串
     * @return 如果是有效的URL安全的Base64编码返回true，否则返回false
     */
    public static boolean isUrlSafeBase64(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        
        if (str.length() % 4 != 0) {
            return false;
        }
        
        if (!BASE64_URL_PATTERN.matcher(str).matches()) {
            return false;
        }
        
        try {
            Base64.getUrlDecoder().decode(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}