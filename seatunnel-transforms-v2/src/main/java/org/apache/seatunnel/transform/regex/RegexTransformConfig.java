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

package org.apache.seatunnel.transform.regex;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@Getter
@Setter
public class RegexTransformConfig implements Serializable {
    public static final Option<String> KEY_REGEX =
            Options.key("regex")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The regex to patten the field");

    public static final Option<String> KEY_REGEX_FIELD =
            Options.key("regex_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field to be regex");

    public static final Option<List<String>> KEY_OUTPUT_FIELDS =
            Options.key("output_fields")
                    .listType()
                    .defaultValue(Collections.EMPTY_LIST)
                    .withDescription("The result fields after regex");

    private String regex;
    private String regexField;
    private String[] outputFields;
    private String[] emptyRegexes;

    public static RegexTransformConfig of(ReadonlyConfig config) {
        RegexTransformConfig regexTransformConfig = new RegexTransformConfig();
        regexTransformConfig.setRegex(config.get(KEY_REGEX));
        regexTransformConfig.setRegexField(config.get(KEY_REGEX_FIELD));
        regexTransformConfig.setOutputFields(config.get(KEY_OUTPUT_FIELDS).toArray(new String[0]));
        regexTransformConfig.setEmptyRegexes(
                new String[regexTransformConfig.getOutputFields().length]);
        return regexTransformConfig;
    }
}
