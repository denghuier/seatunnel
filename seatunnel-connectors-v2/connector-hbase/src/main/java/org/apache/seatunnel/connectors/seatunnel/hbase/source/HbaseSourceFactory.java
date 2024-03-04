package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.*;
@Slf4j
@AutoService(Factory.class)
public class HbaseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Hbase";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS, ZOOKEEPER_QUORUM, TABLE)
                .optional(
                        VERSION_COLUMN,
                        WRITE_BUFFER_SIZE,
                        ENCODING,
                        HBASE_EXTRA_CONFIG)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HbaseSource.class;
    }
}
