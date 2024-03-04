package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseSourceSplitEnumerator  implements SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> {

    private static final Logger log = LoggerFactory.getLogger(HbaseSourceSplitEnumerator.class);

    private SourceSplitEnumerator.Context<HbaseSourceSplit> context;

    private Config pluginConfig;

    private Map<Integer, List<HbaseSourceSplit>> pendingSplit;

    private Map<String, String> familyName;

    private volatile boolean shouldEnumerate;
    private final Object stateLock = new Object();

    public HbaseSourceSplitEnumerator(Context<HbaseSourceSplit> enumeratorContext, Config pluginConfig, Map<String, String> familyName) {

        this(enumeratorContext,pluginConfig,familyName,null);
    }

    public HbaseSourceSplitEnumerator(Context<HbaseSourceSplit> enumeratorContext, Config pluginConfig, Map<String, String> familyName,HbaseSourceState sourceState) {

        this.context = enumeratorContext;
        this.pluginConfig = pluginConfig;
        this.pendingSplit = new HashMap<>();
        this.familyName = familyName;
        this.shouldEnumerate = sourceState == null;
        if(sourceState!=null){
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {

    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        if (shouldEnumerate) {
            List<HbaseSourceSplit> newSplits = getHbaseSplit();

            synchronized (stateLock) {
                addPendingSplit(newSplits);
                shouldEnumerate = false;
            }

            assignSplit(readers);
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private List<HbaseSourceSplit> getHbaseSplit() {
        List<HbaseSourceSplit> splits = new ArrayList<>();
        splits.add(new HbaseSourceSplit("0"));
        return splits;
    }

    private void assignSplit(Collection<Integer> readers) {
        for (int reader : readers) {
            List<HbaseSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private void addPendingSplit(List<HbaseSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (HbaseSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }
    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }
    @Override
    public void close() throws IOException {

    }

    /**
     * 将拆分好的split下发到对应的Reader
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<HbaseSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public HbaseSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new HbaseSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
