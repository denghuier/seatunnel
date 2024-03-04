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

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.*;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.*;

@Slf4j
public class HbaseSourceReader
        implements SourceReader<SeaTunnelRow, HbaseSourceSplit> {


    Context context;

    private Config pluginConfig;


    private HbaseClient hbaseClient;

    Deque<HbaseSourceSplit> splits = new LinkedList<>();

    boolean noMoreSplit;

    private  SeaTunnelRowType rowTypeInfo;

    private final long pollNextWaitTime = 1000L;

    private String tableName;
    public HbaseSourceReader(
            Context context, Config pluginConfig, SeaTunnelRowType rowTypeInfo) {
        this.context = context;
        this.pluginConfig = pluginConfig;
//        this.hbaseParameters = HbaseParameters.buildWithConfig(pluginConfig);
        this.rowTypeInfo = rowTypeInfo;
        this.tableName=pluginConfig.getString(TABLE.key());
    }

    @Override
    public void open() {
        Map<String,String> hosts= TypesafeConfigUtils.configToMap(pluginConfig.getConfig(HOSTS.key()));
        String zk=pluginConfig.getString(ZOOKEEPER_QUORUM.key());
        hbaseClient = HbaseClient.createInstance(hosts,zk);
    }

    @Override
    public void close() throws IOException {
        hbaseClient.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {

        HbaseSourceSplit split = splits.poll();
        if(split !=null){
            ResultScanner resultScanner=hbaseClient.getScanner(tableName);

            for (Result rs : resultScanner){
                Object[] seaTunnelFields = new Object[rowTypeInfo.getTotalFields()];
                Cell[] cells = rs.rawCells();
                for (int fieldIndex = 0; fieldIndex < rowTypeInfo.getTotalFields(); fieldIndex++) {
                    Cell cell=cells[fieldIndex];
                    seaTunnelFields[fieldIndex] = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
                String rowKey = Bytes.toString(rs.getRow());
                System.out.println("row key :" + rowKey);

                output.collect(new SeaTunnelRow(seaTunnelFields));
            }

        } else if (noMoreSplit){
            context.signalNoMoreElement();
        } else {
            Thread.sleep(pollNextWaitTime);
        }
    }

    @Override
    public List<HbaseSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    /**
     * 用于接收枚举器下发的split
     * @param splits split checkpoint state.
     */
    @Override
    public void addSplits(List<HbaseSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
