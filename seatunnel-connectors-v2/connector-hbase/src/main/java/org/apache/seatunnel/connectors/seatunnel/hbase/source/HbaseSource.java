package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.FAMILY_NAME;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.HOSTS;


@AutoService(SeaTunnelSource.class)
public class HbaseSource
        implements SeaTunnelSource<SeaTunnelRow,HbaseSourceSplit,HbaseSourceState>,
        SupportParallelism,
        SupportColumnProjection {


    private Config pluginConfig;

    private SeaTunnelRowType rowTypeInfo;

    private Map<String,String> familyName;
    @Override
    public String getPluginName() {
        return "Hbase";
    }


    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key())) {
            rowTypeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        }
        Map<String,String> hosts=pluginConfig.getConfig(HbaseConfig.HOSTS.key()).entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> String.valueOf(entry.getValue().unwrapped()),
                                (v1, v2) -> v2));
        familyName=TypesafeConfigUtils.configToMap(pluginConfig.getConfig(FAMILY_NAME.key()));

        Map<String,String> hosts1= TypesafeConfigUtils.configToMap(pluginConfig.getConfig(HOSTS.key()));
//        HbaseParameters parameters=HbaseParameters.buildWithConfig(pluginConfig);
//        System.out.println(parameters);
//        familyName = pluginConfig.get(HbaseConfig.FAMILY_NAME.key());

        //判断是否存在“schema”字段
//        CheckResult result =
//                CheckConfigUtil.checkAllExists(pluginConfig, CatalogTableUtil.SCHEMA.key());
        //获取字段类型与字段名称
//        SeaTunnelRowType seaTunnelRowType=CatalogTableUtil.buildSimpleTextSchema();
//        rowTypeInfo=seaTunnelRowType;

    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, HbaseSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new HbaseSourceReader(readerContext,pluginConfig, rowTypeInfo);
    }

    @Override
    public SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> createEnumerator(SourceSplitEnumerator.Context<HbaseSourceSplit> enumeratorContext) throws Exception {
        return new HbaseSourceSplitEnumerator(enumeratorContext,pluginConfig,familyName);
    }

    @Override
    public SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> restoreEnumerator(SourceSplitEnumerator.Context<HbaseSourceSplit> enumeratorContext, HbaseSourceState checkpointState) throws Exception {
        return new HbaseSourceSplitEnumerator(enumeratorContext,pluginConfig,familyName,checkpointState);
    }
}
