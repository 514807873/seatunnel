package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class EncryptTransformConfig implements Serializable {
    public static final Option<List<String>> ENCRYPT_COLUMNS =
            Options.key("encrypt_columns").listType().noDefaultValue().withDescription("对加密字段");
    private List<String> encryptColumns = new ArrayList<>();

    public static EncryptTransformConfig of(ReadonlyConfig config) {
        EncryptTransformConfig encryptTransformConfig = new EncryptTransformConfig();
        encryptTransformConfig.setEncryptColumns(config.get(ENCRYPT_COLUMNS));
        return encryptTransformConfig;
    }
}
