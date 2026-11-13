package org.apache.seatunnel.connectors.seatunnel.groovy.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class GroovySourceOptions {

    public static final String CONNECTOR_IDENTITY = "Groovy";

    public static final Option<String> CODE =
            Options.key("code")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Groovy class source that implements GroovySourceScript, with open and pollNext");

    private GroovySourceOptions() {}
}
