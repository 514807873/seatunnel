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

package org.apache.seatunnel.transform.groupConcat;

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
public class GroupConcatTransformConfig implements Serializable {
    public static final Option<List<String>> KEY_COLUMN =
            Options.key("key_column")
                    .listType(String.class)
                    .noDefaultValue()
                    .withDescription("主键字段配置信息");

    public static final Option<List<JSONObject>> MERGE_COLUMN =
            Options.key("merge_column")
                    .listType(JSONObject.class)
                    .noDefaultValue()
                    .withDescription("合并字段配置信息");

    private List<String> keyColumn;
    private List<JSONObject> mergeColumn;

    public static GroupConcatTransformConfig of(ReadonlyConfig config) {
        GroupConcatTransformConfig splitTransformConfig = new GroupConcatTransformConfig();
        splitTransformConfig.setKeyColumn(config.get(KEY_COLUMN));
        splitTransformConfig.setMergeColumn(config.get(MERGE_COLUMN));
        return splitTransformConfig;
    }
}
