package org.apache.seatunnel.transform.http;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class HttpTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "http_transform";
    }

    @Override
    public OptionRule optionRule() {
        return getHttpBuilder().build();
    }

    public OptionRule.Builder getHttpBuilder() {
        return OptionRule.builder()
                .required(HttpTransformConfig.URL)
                .optional(HttpTransformConfig.PAGES_PARAMS)
                .optional(HttpTransformConfig.PARAMS_FIELD);
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        ReadonlyConfig options = context.getOptions();
        HttpTransformConfig httpTransformConfig = HttpTransformConfig.of(options);
        return () -> new HttpTransform(httpTransformConfig, catalogTable);
    }
}
