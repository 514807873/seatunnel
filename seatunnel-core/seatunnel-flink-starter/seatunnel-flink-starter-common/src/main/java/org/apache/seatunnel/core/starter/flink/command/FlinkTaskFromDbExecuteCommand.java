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

package org.apache.seatunnel.core.starter.flink.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

@Slf4j
public class FlinkTaskFromDbExecuteCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkTaskFromDbExecuteCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Config config = ConfigFactory.parseString(getConfig(), ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(
                        ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
        config = ConfigShadeUtils.decryptConfig(config);

        // if user specified job name using command line arguments, override config option
        if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
        }
        FlinkExecution seaTunnelTaskExecution = new FlinkExecution(config, this.flinkCommandArgs.getFlinkJobId());
        try {
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            throw new CommandExecuteException("Flink job executed failed", e);
        }
    }

    private String getConfig() {
        try {
            String postData = String.format("{\"jobId\":\"%s\"}", new Object[]{this.flinkCommandArgs.getConfigFile()});
            String serviceUrl = System.getenv("ST_SERVICE_URL");
            if (StringUtils.isEmpty(serviceUrl)) {
                serviceUrl = "http://pangu-seatunnel:8872";
            }
            log.info("st_config_file_url: %s", serviceUrl);
            String st_config_file_url = serviceUrl + "/SeaTunnelJob/stConfigString";
            StringBuilder stringBuilder = new StringBuilder();
            URL apiUrl = new URL(st_config_file_url);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(postData.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null)
                    stringBuilder.append(line).append("\n");
                reader.close();
//                System.out.println("Response Body:\n" + stringBuilder.toString());
                connection.disconnect();
                return stringBuilder.toString();
            }
            connection.disconnect();
            throw new RuntimeException("获取作业JSON文件失败,请检查 ST_SERVICE_URL 环境变量");
        } catch (IOException e) {
            throw new RuntimeException("获取作业JSON文件失败,请检查 ST_SERVICE_URL 环境变量", e);
        }
    }
}
