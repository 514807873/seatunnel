package org.apache.seatunnel.core.starter.flink.utils;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public final class HttpUtil {
    private HttpUtil() {}

    public static void sendPostRequest(String url, String data) throws Exception {
        URL apiUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);

        OutputStream outputStream = connection.getOutputStream();
        outputStream.write(data.getBytes());
        outputStream.flush();
        outputStream.close();

        // 处理响应
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            // 成功处理响应
        } else {
            // 处理失败响应
        }
        connection.disconnect();
    }
}
