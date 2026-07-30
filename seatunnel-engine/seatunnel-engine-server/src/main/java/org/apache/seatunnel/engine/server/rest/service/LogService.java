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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple3;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_GET_ALL_LOG_NAME;
import static org.apache.seatunnel.engine.server.rest.RestConstant.REST_URL_LOGS;

@Slf4j
public class LogService extends BaseLogService {
    public static final String LOG_PROXY_PREFIX = "proxy/";

    public LogService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    public List<String> allLogName() {
        String logPath = getLogPath();
        List<File> logFileList = FileUtils.listFile(logPath);
        if (logFileList == null) {
            return new ArrayList<>();
        }
        return logFileList.stream().map(File::getName).collect(Collectors.toList());
    }

    public List<Tuple3<String, String, String>> allLogNameList(String jobId) {

        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        int port = httpConfig.getPort();
        String publicUrl = normalizePublicUrl(httpConfig.getPublicUrl());

        List<Tuple3<String, String, String>> allLogNameList = new ArrayList<>();

        JsonArray systemMonitoringInformationJsonValues =
                getSystemMonitoringInformationJsonValues();
        systemMonitoringInformationJsonValues.forEach(
                systemMonitoringInformation -> {
                    String host = systemMonitoringInformation.asObject().get("host").asString();
                    String node = host + ":" + port;
                    String directBaseUrl = "http://" + node + contextPath;
                    String logUrl = directBaseUrl + REST_URL_GET_ALL_LOG_NAME;

                    String allName =
                            httpConfig.isEnableBasicAuth()
                                    ? sendGet(
                                            logUrl,
                                            httpConfig.getBasicAuthUsername(),
                                            httpConfig.getBasicAuthPassword())
                                    : sendGet(logUrl);

                    if (StringUtils.isBlank(allName)) {
                        log.warn(
                                "GET {} returned empty body (null/empty). Skip this node.", logUrl);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Request: {} , Result: {}", directBaseUrl, allName);
                    }
                    ArrayNode jsonNodes = JsonUtils.parseArray(allName);

                    jsonNodes.forEach(
                            jsonNode -> {
                                String fileName = jsonNode.asText();
                                if (StringUtils.isNotBlank(jobId) && !fileName.contains(jobId)) {
                                    return;
                                }
                                String logLink =
                                        buildLogContentLink(
                                                publicUrl, contextPath, directBaseUrl, node, fileName);
                                allLogNameList.add(new Tuple3<>(node, logLink, fileName));
                            });
                });

        return allLogNameList;
    }

    /**
     * When {@code publicUrl} is set (e.g. K8s NodePort), return a link that goes through this
     * gateway and is reverse-proxied to the real node. Otherwise keep the direct node URL.
     */
    static String buildLogContentLink(
            String publicUrl, String contextPath, String directBaseUrl, String node, String fileName) {
        if (StringUtils.isNotBlank(publicUrl)) {
            return publicUrl + contextPath + REST_URL_LOGS + "/" + LOG_PROXY_PREFIX + node + "/" + fileName;
        }
        return directBaseUrl + REST_URL_LOGS + "/" + fileName;
    }

    static String normalizePublicUrl(String publicUrl) {
        if (StringUtils.isBlank(publicUrl)) {
            return "";
        }
        String trimmed = publicUrl.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    /**
     * Parse {@code proxy/{host}:{port}/{fileName}} from the path after {@code /logs/}.
     *
     * @return Optional of [node, fileName]
     */
    public static Optional<String[]> parseProxyPath(String logParam) {
        if (StringUtils.isBlank(logParam) || !logParam.startsWith(LOG_PROXY_PREFIX)) {
            return Optional.empty();
        }
        String rest = logParam.substring(LOG_PROXY_PREFIX.length());
        int slash = rest.indexOf('/');
        if (slash <= 0 || slash >= rest.length() - 1) {
            return Optional.empty();
        }
        String node = rest.substring(0, slash);
        String fileName = rest.substring(slash + 1);
        if (!node.contains(":") || !fileName.endsWith(".log") || fileName.contains("..") || fileName.contains("/")) {
            return Optional.empty();
        }
        return Optional.of(new String[] {node, fileName});
    }

    /**
     * Fetch log content from a cluster-internal node. Used by the public-url reverse proxy.
     */
    public String fetchNodeLogContent(String node, String fileName) {
        SeaTunnelServer seaTunnelServer = getSeaTunnelServer(false);
        HttpConfig httpConfig =
                seaTunnelServer.getSeaTunnelConfig().getEngineConfig().getHttpConfig();
        String contextPath = httpConfig.getContextPath();
        String url = "http://" + node + contextPath + REST_URL_LOGS + "/" + fileName;
        if (httpConfig.isEnableBasicAuth()) {
            return sendGet(
                    url,
                    httpConfig.getBasicAuthUsername(),
                    httpConfig.getBasicAuthPassword(),
                    5000,
                    60000);
        }
        return sendGet(url, null, null, 5000, 60000);
    }

    public JsonArray allNodeLogFormatJson(String jobId) {

        return allLogNameList(jobId).stream()
                .map(
                        tuple -> {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.add("node", tuple._1());
                            jsonObject.add("logLink", tuple._2());
                            jsonObject.add("logName", tuple._3());
                            return jsonObject;
                        })
                .collect(JsonArray::new, JsonArray::add, JsonArray::add);
    }

    public String allNodeLogFormatHtml(String jobId) {
        StringBuffer logLink = new StringBuffer();

        allLogNameList(jobId)
                .forEach(tuple -> logLink.append(buildLogLink(tuple._2(), tuple._3())));
        return buildWebSiteContent(logLink);
    }

    public String currentNodeLog() {
        List<File> logFileList = FileUtils.listFile(getLogPath());
        StringBuffer logLink = new StringBuffer();
        if (logFileList != null) {
            for (File file : logFileList) {
                logLink.append(buildLogLink("log/" + file.getName(), file.getName()));
            }
        }

        return buildWebSiteContent(logLink);
    }
}
