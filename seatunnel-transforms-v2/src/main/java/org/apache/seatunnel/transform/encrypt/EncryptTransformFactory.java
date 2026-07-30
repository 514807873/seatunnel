package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.copy.CopyTransformConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class EncryptTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "Encrypt";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(EncryptTransformConfig.ENCRYPT_COLUMNS).build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        CopyTransformConfig copyTransformConfig = CopyTransformConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        ReadonlyConfig options = context.getOptions();
        EncryptTransformConfig encryptTransformConfig = EncryptTransformConfig.of(options);
        return () -> new EncryptTransform(encryptTransformConfig, catalogTable);
    }
}
