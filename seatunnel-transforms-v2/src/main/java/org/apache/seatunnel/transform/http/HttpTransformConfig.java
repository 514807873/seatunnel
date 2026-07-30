package org.apache.seatunnel.transform.http;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class HttpTransformConfig implements Serializable {

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("Http request url");
    public static final Option<Map<String, String>> PAGES_PARAMS =
            Options.key("pages").mapType().noDefaultValue().withDescription("分页参数");
    public static final Option<List<HttpParam>> PARAMS_FIELD =
            Options.key("params_field")
                    .listType(HttpParam.class)
                    .noDefaultValue()
                    .withDescription("绑定参数");
    public static final Option<Map<String, String>> JSON_FIELD =
            Options.key("json_field").mapType().noDefaultValue().withDescription("jpath参数列表");

    private String url;
    private List<HttpParam> paramsField;
    private Map<String, String> pagesParams;
    private Map<String, String> jsonField;

    public static HttpTransformConfig of(ReadonlyConfig options) {
        HttpTransformConfig httpTransformConfig = new HttpTransformConfig();
        httpTransformConfig.setUrl(options.get(URL));
        httpTransformConfig.setParamsField(options.get(PARAMS_FIELD));
        httpTransformConfig.setPagesParams(options.get(PAGES_PARAMS));
        httpTransformConfig.setJsonField(options.get(JSON_FIELD));
        return httpTransformConfig;
    }
}
