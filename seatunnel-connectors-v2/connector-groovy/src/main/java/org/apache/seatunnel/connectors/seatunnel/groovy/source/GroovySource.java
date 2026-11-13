package org.apache.seatunnel.connectors.seatunnel.groovy.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.groovy.config.GroovySourceOptions;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class GroovySource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final String code;
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public GroovySource(Config pluginConfig) {
        if (pluginConfig == null || !pluginConfig.hasPath(GroovySourceOptions.CODE.key())) {
            throw new SeaTunnelException("Groovy source requires option [code]");
        }
        if (!pluginConfig.hasPath(TableSchemaOptions.SCHEMA.key())) {
            throw new SeaTunnelException("Groovy source requires option [schema]");
        }
        this.code = pluginConfig.getString(GroovySourceOptions.CODE.key());
        if (this.code == null || this.code.trim().isEmpty()) {
            throw new SeaTunnelException("Groovy source option [code] is empty");
        }
        this.catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return GroovySourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        return new GroovySourceReader(
                code,
                readerContext,
                Arrays.asList(catalogTable.getTableSchema().getFieldNames()));
    }
}
