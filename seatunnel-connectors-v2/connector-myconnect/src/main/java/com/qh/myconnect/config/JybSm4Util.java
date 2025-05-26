package com.qh.myconnect.config;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文件描述：国密算法SM4 对称加密实现
 * 教育部大数据中心提供
 */
public class JybSm4Util {
    //加密的key 统一下发
    private  String SM4_KEY = "您下发的key";


    public JybSm4Util() {

    }

    public JybSm4Util(String keyStr) {
        this.SM4_KEY = keyStr;
    }

    // 固定参数常量
    private static final int[] SM4_CK = {
            0x00070e15, 0x1c232a31, 0x383f464d, 0x545b6269,
            0x70777e85, 0x8c939aa1, 0xa8afb6bd, 0xc4cbd2d9,
            0xe0e7eef5, 0xfc030a11, 0x181f262d, 0x343b4249,
            0x50575e65, 0x6c737a81, 0x888f969d, 0xa4abb2b9,
            0xc0c7ced5, 0xdce3eaf1, 0xf8ff060d, 0x141b2229,
            0x30373e45, 0x4c535a61, 0x686f767d, 0x848b9299,
            0xa0a7aeb5, 0xbcc3cad1, 0xd8dfe6ed, 0xf4fb0209,
            0x10171e25, 0x2c333a41, 0x484f565d, 0x646b7279
    };
    // 固定参数常量
    private static final int[] SM4_SBOX = {
            0xd6, 0x90, 0xe9, 0xfe, 0xcc, 0xe1, 0x3d, 0xb7, 0x16, 0xb6, 0x14, 0xc2, 0x28, 0xfb, 0x2c, 0x05,
            0x2b, 0x67, 0x9a, 0x76, 0x2a, 0xbe, 0x04, 0xc3, 0xaa, 0x44, 0x13, 0x26, 0x49, 0x86, 0x06, 0x99,
            0x9c, 0x42, 0x50, 0xf4, 0x91, 0xef, 0x98, 0x7a, 0x33, 0x54, 0x0b, 0x43, 0xed, 0xcf, 0xac, 0x62,
            0xe4, 0xb3, 0x1c, 0xa9, 0xc9, 0x08, 0xe8, 0x95, 0x80, 0xdf, 0x94, 0xfa, 0x75, 0x8f, 0x3f, 0xa6,
            0x47, 0x07, 0xa7, 0xfc, 0xf3, 0x73, 0x17, 0xba, 0x83, 0x59, 0x3c, 0x19, 0xe6, 0x85, 0x4f, 0xa8,
            0x68, 0x6b, 0x81, 0xb2, 0x71, 0x64, 0xda, 0x8b, 0xf8, 0xeb, 0x0f, 0x4b, 0x70, 0x56, 0x9d, 0x35,
            0x1e, 0x24, 0x0e, 0x5e, 0x63, 0x58, 0xd1, 0xa2, 0x25, 0x22, 0x7c, 0x3b, 0x01, 0x21, 0x78, 0x87,
            0xd4, 0x00, 0x46, 0x57, 0x9f, 0xd3, 0x27, 0x52, 0x4c, 0x36, 0x02, 0xe7, 0xa0, 0xc4, 0xc8, 0x9e,
            0xea, 0xbf, 0x8a, 0xd2, 0x40, 0xc7, 0x38, 0xb5, 0xa3, 0xf7, 0xf2, 0xce, 0xf9, 0x61, 0x15, 0xa1,
            0xe0, 0xae, 0x5d, 0xa4, 0x9b, 0x34, 0x1a, 0x55, 0xad, 0x93, 0x32, 0x30, 0xf5, 0x8c, 0xb1, 0xe3,
            0x1d, 0xf6, 0xe2, 0x2e, 0x82, 0x66, 0xca, 0x60, 0xc0, 0x29, 0x23, 0xab, 0x0d, 0x53, 0x4e, 0x6f,
            0xd5, 0xdb, 0x37, 0x45, 0xde, 0xfd, 0x8e, 0x2f, 0x03, 0xff, 0x6a, 0x72, 0x6d, 0x6c, 0x5b, 0x51,
            0x8d, 0x1b, 0xaf, 0x92, 0xbb, 0xdd, 0xbc, 0x7f, 0x11, 0xd9, 0x5c, 0x41, 0x1f, 0x10, 0x5a, 0xd8,
            0x0a, 0xc1, 0x31, 0x88, 0xa5, 0xcd, 0x7b, 0xbd, 0x2d, 0x74, 0xd0, 0x12, 0xb8, 0xe5, 0xb4, 0xb0,
            0x89, 0x69, 0x97, 0x4a, 0x0c, 0x96, 0x77, 0x7e, 0x65, 0xb9, 0xf1, 0x09, 0xc5, 0x6e, 0xc6, 0x84,
            0x18, 0xf0, 0x7d, 0xec, 0x3a, 0xdc, 0x4d, 0x20, 0x79, 0xee, 0x5f, 0x3e, 0xd7, 0xcb, 0x39, 0x48
    };

    // 系统参数常量
    private static final int[] SM4_FK = {0xA3B1BAC6, 0x56AA3350, 0x677D9197, 0xB27022DC};

    private List<Integer> key = new ArrayList<>(); // 16个 HEX格式的数组 16字节 128bits，存储为十进制
    private List<Integer> skey = new ArrayList<>(); // 记录每轮加密的秘钥，存储为十进制
    private final int block_size = 32; // 块大小，固定为32

    /**
     * 设置加密秘钥
     *
     * @param keyStr 32个十六进制的字符
     * @return 当前SM4对象
     * @throws Exception 如果输入格式错误则抛出异常
     */
    public JybSm4Util setKey(String keyStr) throws Exception {
        // 截取并转换为小写
        String processedKey = keyStr.toLowerCase().substring(4, 24);
        // 将字符串转换为十六进制表示
        String hexKey = stringToHex(processedKey);
        // 预处理密钥
        this.key = preProcess(hexKey);
        // 设置每轮秘钥
        setSkey();
        return this;
    }

    /**
     * 计算每轮加密需要的秘钥
     */
    private void setSkey() {
        List<Integer> tempSkey = new ArrayList<>();
        // 初始秘钥计算
        for (int i = 0; i < 4; i++) {
            int temp = SM4_FK[i] ^ (key.get(4 * i) << 24 | key.get(4 * i + 1) << 16 | key.get(4 * i + 2) << 8 | key.get(4 * i + 3));
            tempSkey.add(temp);
        }
        // 32轮秘钥生成
        for (int k = 0; k < 32; k++) {
            int tmp = tempSkey.get(k + 1) ^ tempSkey.get(k + 2) ^ tempSkey.get(k + 3) ^ SM4_CK[k];
            // 非线性化操作
            int buf = (SM4_SBOX[(tmp >> 24) & 0xff] << 24) |
                      (SM4_SBOX[(tmp >> 16) & 0xff] << 16) |
                      (SM4_SBOX[(tmp >> 8) & 0xff] << 8) |
                      SM4_SBOX[tmp & 0xff];
            // 线性化操作
            int newKey = tempSkey.get(k) ^ buf ^ sm4Rotl32(buf, 13) ^ sm4Rotl32(buf, 23);
            tempSkey.add(newKey);
            skey.add(tempSkey.get(k + 4));
        }
    }

    /**
     * 32比特的buffer中循环左移n位
     *
     * @param buf 32位整数
     * @param n   左移位数
     * @return 循环左移后的结果
     */
    private int sm4Rotl32(int buf, int n) {
        return ((buf << n) & 0xffffffff) | (buf >>> (32 - n));
    }

    /**
     * 对字符串加密
     *
     * @param plainText 明文字符串
     * @return 加密后的十六进制字符串
     * @throws Exception 如果输入格式错误则抛出异常
     */
    public String encryptEcb(String key, String plainText) throws Exception {
        this.setKey(key);
        // 将明文字符串转换为十六进制字符串
        String bytes = stringToHex(plainText);
        // 计算需要填充的长度，以确保数据块大小符合加密要求
        int needPadLength = block_size - (bytes.length() % block_size);
        // 对数据进行填充
        String padBytes = padBytes(bytes, needPadLength);
        // 将填充后的数据分块，每块大小为block_size
        String[] chunks = splitString(padBytes, block_size);
        // 加密每个数据块，并将结果拼接成一个字符串
        StringBuilder encrypted = new StringBuilder();
        for (String chunk : chunks) {
            encrypted.append(encrypt(chunk));
        }
        return encrypted.toString();
    }
    public String encryptEcb( String plainText) throws Exception {
        this.setKey(this.SM4_KEY);
        // 将明文字符串转换为十六进制字符串
        String bytes = stringToHex(plainText);
        // 计算需要填充的长度，以确保数据块大小符合加密要求
        int needPadLength = block_size - (bytes.length() % block_size);
        // 对数据进行填充
        String padBytes = padBytes(bytes, needPadLength);
        // 将填充后的数据分块，每块大小为block_size
        String[] chunks = splitString(padBytes, block_size);
        // 加密每个数据块，并将结果拼接成一个字符串
        StringBuilder encrypted = new StringBuilder();
        for (String chunk : chunks) {
            encrypted.append(encrypt(chunk));
        }
        return encrypted.toString();
    }

    /**
     * 将字节字符串填充到指定长度
     * 对字节字符串的填充操作，确保字符串达到特定长度
     * 它通过在原字符串后添加计算得出的填充值来实现这一点
     *
     * @param bytes         原始字节字符串，代表需要填充的字节数据
     * @param needPadLength 需要填充的总长度，包括原始字符串长度和额外的填充长度
     * @return 填充后的字节字符串，其长度等于原始字符串长度加上所需的填充长度
     */
    public static String padBytes(String bytes, int needPadLength) {
        // 根据needPadLength计算填充的十六进制字符串
        String paddingValue = String.format("%02x", needPadLength / 2);
        // 创建StringBuilder以动态构建最终的填充字符串
        StringBuilder result = new StringBuilder(bytes);
        // 循环添加填充值，直到达到所需的总长度
        while (result.length() < bytes.length() + needPadLength) {
            result.append(paddingValue);
        }
        // 返回填充后的字符串，确保其长度正好为原始长度加上填充长度
        return result.toString().substring(0, bytes.length() + needPadLength);
    }

    /**
     * 解密数据
     * <p>
     * 该方法负责将给定的密文（cipherText）解密回原始的明文字符串它首先将密文分块处理，
     * 然后对每一块进行解密解密后的数据被转换为字节数组，并检查是否有PKCS#7填充，
     * 如果有，则移除最后，将处理后的字节数组转换为字符串并返回
     *
     * @param cipherText 加密后的密文字符串
     * @return 解密后的明文字符串
     * @throws Exception 如果解密过程中发生错误
     */
    public String decryptEcb(String key, String cipherText) throws Exception {
        this.setKey(key);
        // 分块解密
        String[] chunks = splitString(cipherText, block_size);
        StringBuilder decryptTextData = new StringBuilder();
        for (String chunk : chunks) {
            decryptTextData.append(decrypt(chunk));
        }

        // 将解密得到的hex字符串转为byte数组
        byte[] bytes = hexToBytes(decryptTextData.toString());

        // 检测并移除PKCS#7填充
        int padLength = bytes[bytes.length - 1] & 0xFF; // 最后一个字节表示填充长度
        if (padLength > 0 && padLength <= bytes.length) {
            boolean validPadding = true;
            for (int i = 0; i < padLength; i++) {
                if (bytes[bytes.length - 1 - i] != (byte) padLength) {
                    validPadding = false;
                    break;
                }
            }

            if (validPadding) {
                // 去掉末尾的填充字节
                bytes = java.util.Arrays.copyOf(bytes, bytes.length - padLength);
            }
        }

        // 不使用trim()，保证原有空格不被移除
        return new String(bytes, StandardCharsets.UTF_8);
    }
    public String decryptEcb(String cipherText) throws Exception {
        this.setKey(this.SM4_KEY);
        // 分块解密
        String[] chunks = splitString(cipherText, block_size);
        StringBuilder decryptTextData = new StringBuilder();
        for (String chunk : chunks) {
            decryptTextData.append(decrypt(chunk));
        }

        // 将解密得到的hex字符串转为byte数组
        byte[] bytes = hexToBytes(decryptTextData.toString());

        // 检测并移除PKCS#7填充
        int padLength = bytes[bytes.length - 1] & 0xFF; // 最后一个字节表示填充长度
        if (padLength > 0 && padLength <= bytes.length) {
            boolean validPadding = true;
            for (int i = 0; i < padLength; i++) {
                if (bytes[bytes.length - 1 - i] != (byte) padLength) {
                    validPadding = false;
                    break;
                }
            }

            if (validPadding) {
                // 去掉末尾的填充字节
                bytes = java.util.Arrays.copyOf(bytes, bytes.length - padLength);
            }
        }

        // 不使用trim()，保证原有空格不被移除
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 将hex字符串转换为字节数组的辅助方法
     * 此方法用于处理从hex字符串到字节数组的转换，常用于密码学操作或数据解析
     *
     * @param hexString 待转换的hex字符串，应为偶数长度且仅包含0-9和A-F（不区分大小写）
     * @return 转换后的字节数组，长度为输入字符串长度的一半
     */
    private static byte[] hexToBytes(String hexString) {
        // 获取输入字符串长度
        int len = hexString.length();
        // 根据hex字符串长度初始化字节数组
        byte[] data = new byte[len / 2];
        // 遍历hex字符串，每两个字符转换为一个字节
        for (int i = 0; i < len; i += 2) {
            // 将每两个hex字符转换为对应的字节值
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                                  + Character.digit(hexString.charAt(i + 1), 16));
        }
        // 返回转换后的字节数组
        return data;
    }

    /**
     * SM4加密单个片段(128bit)
     *
     * @param text 32个十六进制字符串
     * @return 加密后的十六进制字符串
     * @throws Exception 如果输入格式错误则抛出异常
     */
    public String encrypt(String text) throws Exception {
        // 初始化存储加密过程数据的列表
        List<Integer> x = new ArrayList<>();
        List<Integer> re = new ArrayList<>();
        // 对输入文本进行预处理
        List<Integer> t = preProcess(text);

        // 将输入文本转换为32位整数
        for (int i = 0; i < 4; i++) {
            int temp = (t.get(i * 4) << 24) |
                       (t.get(i * 4 + 1) << 16) |
                       (t.get(i * 4 + 2) << 8) |
                       t.get(i * 4 + 3);
            x.add(temp);
        }

        // 32轮加密
        for (int k = 0; k < 32; k++) {
            // 计算每一轮的临时变量
            int tmp = x.get(k + 1) ^ x.get(k + 2) ^ x.get(k + 3) ^ skey.get(k);
            // 通过S盒替换和循环移位操作生成新的加密数据
            int buf = (SM4_SBOX[(tmp >> 24) & 0xff] << 24) |
                      (SM4_SBOX[(tmp >> 16) & 0xff] << 16) |
                      (SM4_SBOX[(tmp >> 8) & 0xff] << 8) |
                      SM4_SBOX[tmp & 0xff];
            int newX = x.get(k) ^ buf ^ sm4Rotl32(buf, 2) ^ sm4Rotl32(buf, 10) ^ sm4Rotl32(buf, 18) ^ sm4Rotl32(buf, 24);
            x.add(newX);
        }

        // 逆序输出
        for (int i = 0; i < 4; i++) {
            int value = x.get(35 - i);
            re.add((value >> 24) & 0xff);
            re.add((value >> 16) & 0xff);
            re.add((value >> 8) & 0xff);
            re.add(value & 0xff);
        }

        // 返回加密后的结果
        return wrapResult(re);
    }

    /**
     * SM4解密单个片段(128bits)
     *
     * @param text 32个十六进制字符串
     * @return 解密后的十六进制字符串
     * @throws Exception 如果输入格式错误则抛出异常
     */
    public String decrypt(String text) throws Exception {
        // 初始化存储解密过程中的状态字和最终结果的列表
        List<Integer> x = new ArrayList<>();
        List<Integer> re = new ArrayList<>();
        // 对输入的文本进行预处理，转换为便于计算的格式
        List<Integer> t = preProcess(text);

        // 将输入文本转换为32位整数
        for (int i = 0; i < 4; i++) {
            int temp = (t.get(4 * i) << 24) |
                       (t.get(4 * i + 1) << 16) |
                       (t.get(4 * i + 2) << 8) |
                       t.get(4 * i + 3);
            x.add(temp);
        }

        // 32轮解密
        for (int k = 0; k < 32; k++) {
            // 计算轮密钥并进行S盒替换
            int tmp = x.get(k + 1) ^ x.get(k + 2) ^ x.get(k + 3) ^ skey.get(31 - k);
            int buf = (SM4_SBOX[(tmp >> 24) & 0xff] << 24) |
                      (SM4_SBOX[(tmp >> 16) & 0xff] << 16) |
                      (SM4_SBOX[(tmp >> 8) & 0xff] << 8) |
                      SM4_SBOX[tmp & 0xff];
            // 计算新的状态字
            int newX = x.get(k) ^ buf ^ sm4Rotl32(buf, 2) ^ sm4Rotl32(buf, 10) ^ sm4Rotl32(buf, 18) ^ sm4Rotl32(buf, 24);
            x.add(newX);
        }

        // 逆序输出
        for (int i = 0; i < 4; i++) {
            int value = x.get(35 - i);
            re.add((value >> 24) & 0xff);
            re.add((value >> 16) & 0xff);
            re.add((value >> 8) & 0xff);
            re.add(value & 0xff);
        }

        // 将解密后的数据封装为最终的输出格式
        return wrapResult(re);
    }

    /**
     * 预处理16字节长度的16进制字符串，返回十进制的数组，数组大小为16
     *
     * @param text 32个十六进制字符
     * @return 十进制数组
     * @throws Exception 如果输入格式错误则抛出异常
     */
    private List<Integer> preProcess(String text) throws Exception {
        // 编译正则表达式以匹配32位的十六进制字符串
        Pattern pattern = Pattern.compile("[0-9a-f]{32}");
        // 使用正则表达式匹配输入的字符串
        Matcher matcher = pattern.matcher(text.toLowerCase());
        // 如果没有找到匹配项，抛出异常
        if (!matcher.find()) {
            throw new Exception("error input format!");
        }
        // 获取匹配的字符串
        String key = matcher.group(0);
        // 创建一个列表存储转换后的十进制数字
        List<Integer> result = new ArrayList<>();
        // 遍历匹配的字符串，每两个字符转换为一个十进制数字
        for (int i = 0; i < 16; i++) {
            // 提取两个字符作为字节字符串
            String byteStr = key.substring(2 * i, 2 * i + 2);
            // 将字节字符串转换为十进制数字并添加到结果列表中
            result.add(Integer.parseInt(byteStr, 16));
        }
        // 返回结果列表
        return result;
    }

    /**
     * 将十进制结果包装成16进制字符串输出
     *
     * @param result 十进制列表
     * @return 大写的十六进制字符串
     */
    private String wrapResult(List<Integer> result) {
        StringBuilder hexStr = new StringBuilder();
        for (Integer v : result) {
            String tmp = Integer.toHexString(v);
            if (tmp.length() == 1) { // 不足两位十六进制的数，在前面补一个0
                hexStr.append('0');
            }
            hexStr.append(tmp);
        }
        return hexStr.toString().toUpperCase();
    }

    /**
     * 将字符串转换为十六进制表示
     *
     * @param str 输入字符串
     * @return 十六进制字符串
     */
    private String stringToHex(String str) {
        // 将字符串按UTF-8编码为字节数组，然后每个字节转为2位十六进制
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }


    /**
     * 将字符串按指定长度分割
     *
     * @param str    输入字符串
     * @param length 每段长度
     * @return 字符串数组
     */
    private String[] splitString(String str, int length) {
        // 计算结果数组的长度，使用向上取整确保所有字符都被处理
        int arrayLength = (int) Math.ceil((double) str.length() / length);
        // 初始化结果数组
        String[] result = new String[arrayLength];
        // 遍历数组，分割字符串
        for (int i = 0; i < arrayLength; i++) {
            // 计算当前段的起始位置
            int start = i * length;
            // 计算当前段的结束位置，确保不超过字符串长度
            int end = Math.min(start + length, str.length());
            // 截取字符串并存入结果数组
            result[i] = str.substring(start, end);
        }
        // 返回结果数组
        return result;
    }

    /**
     * 主程序入口
     * 演示使用SM4加密和解密的过程
     * 该程序首先定义一个待加密的文本字符串，然后使用SM4加密算法进行加密，
     * 最后对加密后的文本进行解密，展示加密和解密的结果
     *
     * @param args 命令行参数
     * @throws Exception 如果加密或解密过程中发生错误，则抛出异常
     */
    public static void main(String[] args) throws Exception {
        // 待加密文本
        String plainText = "待加密文本";
        System.out.println("原始文本：" + plainText);

        // 加密
        String encryptedText = new JybSm4Util().encryptEcb(plainText);
        System.out.println("加密文本：" + encryptedText);

        //解密
        String decryptedText = new JybSm4Util().decryptEcb(encryptedText);
        System.out.println("解密文本：" + decryptedText);
    }

}
