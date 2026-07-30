package org.apache.seatunnel.connectors.seatunnel.kafka.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtils {
    public static String sendPostRequest(String url, String data) throws Exception {
        URL apiUrl = new URL(url);
        HttpURLConnection con = (HttpURLConnection) apiUrl.openConnection();
        String result = null;
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);
        OutputStream outputStream = con.getOutputStream();
        outputStream.write(data.getBytes());
        outputStream.flush();
        outputStream.close();

        // 处理响应
        int responseCode = con.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {

            InputStream is = con.getInputStream();
            // 缓冲流包装字符输入流,放入内存中,读取效率更快
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            StringBuffer stringBuffer1 = new StringBuffer();
            String line = null;
            while ((line = br.readLine()) != null) {
                // 将每次读取的行进行保存
                stringBuffer1.append(line);
            }
            result = stringBuffer1.toString();
        } else {
            // 处理失败响应
        }
        con.disconnect();
        return result;
    }
}
