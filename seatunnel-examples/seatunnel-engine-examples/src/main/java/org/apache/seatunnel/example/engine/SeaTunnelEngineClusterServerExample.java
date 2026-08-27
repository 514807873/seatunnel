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

package org.apache.seatunnel.example.engine;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;
import org.apache.seatunnel.engine.common.Constant;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SeaTunnelEngineClusterServerExample {

    static {
        // https://logging.apache.org/log4j/2.x/manual/simple-logger.html#isThreadContextMapInheritable
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
        bindRepoSeatunnelYaml();
    }

    /**
     * IDEA 默认从 classpath 读 seatunnel-engine-common 自带的 seatunnel.yaml（hdfs +
     * file:///），Windows 会因没有 HADOOP_HOME 写不了 checkpoint。优先绑仓库 config/seatunnel.yaml。
     */
    private static void bindRepoSeatunnelYaml() {
        if (System.getProperty(Constant.SYSPROP_SEATUNNEL_CONFIG) != null) {
            return;
        }
        Path userDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        Path[] candidates =
                new Path[] {
                    userDir.resolve("config/seatunnel.yaml"),
                    userDir.resolve("../../config/seatunnel.yaml")
                };
        for (Path candidate : candidates) {
            Path yaml = candidate.normalize();
            if (Files.isRegularFile(yaml)) {
                System.setProperty(Constant.SYSPROP_SEATUNNEL_CONFIG, yaml.toString());
                return;
            }
        }
    }

    public static void main(String[] args) throws CommandException {
        ServerCommandArgs serverCommandArgs = new ServerCommandArgs();
        SeaTunnel.run(serverCommandArgs.buildCommand());
    }
}
