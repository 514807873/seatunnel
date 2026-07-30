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

package org.apache.seatunnel.transform.splitColumn;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import cn.hutool.json.JSONObject;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class SplitColumnValueTransformConfig implements Serializable {
    public static final Option<List<JSONObject>> KEY_SEPARATOR =
            Options.key("separator")
                    .listType(JSONObject.class)
                    .noDefaultValue()
                    .withDescription("字段切割配置信息");

    private List<JSONObject> separator;

    public static SplitColumnValueTransformConfig of(ReadonlyConfig config) {
        SplitColumnValueTransformConfig splitTransformConfig =
                new SplitColumnValueTransformConfig();
        splitTransformConfig.setSeparator(config.get(KEY_SEPARATOR));
        return splitTransformConfig;
    }
}
