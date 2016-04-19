package flume.source.plugin;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by jiandaohong on 2015/9/21.
 */

public class ReliableTailSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LogManager.getLogger(ReliableTailSource.class);

    // constants
    private static int POLL_DELAY_MS = 300;

    // config options
    private String spoolDirectory;
    private String offsetDirectory;
    private int batchSize;
    private String ignorePattern;
    private String ignoreContentPattern;
    private String filterContentPattern;
    private String inputCharset;
    private int intervalMillis;
    private DecodeErrorPolicy decodeErrorPolicy;
    private boolean backoff = true;
    private int maxBackoff;
    private int emptyEventsCount = 0;
    private int emptyEventsDelay = 100;
    private int maxEmptyEventDelay = 500;
    private boolean saveOffsetOrNot = false;

    // process info
    private SourceCounter sourceCounter;
    private ReliableTailSourceEventReader reader;
    private ScheduledExecutorService executor;
    private Optional<OffsetInfo> lastReadOffsetInfo = Optional.absent();

    public synchronized void start() {
        logger.info("ReliableTailSource starting with directory:" + spoolDirectory);

        executor = Executors.newSingleThreadScheduledExecutor();

        File directory = new File(spoolDirectory);
        if (!directory.exists() || !directory.isDirectory()) {
            logger.error("spool directory:" + spoolDirectory + " error");
            return;
        }

        // read offset config
        if (saveOffsetOrNot) {
            File offsetDir = new File(offsetDirectory);
            if (!offsetDir.exists() || !offsetDir.isDirectory()) {
                logger.info("offset path not exist. will create");
                offsetDir.mkdir();
            }
            String offsetFileName = offsetDirectory + "/offset";
            File offsetFile = new File(offsetFileName);
            if (!offsetFile.exists()) {
                try {
                    offsetFile.createNewFile();
                } catch (IOException e) {
                    logger.error("cannot create offset file:" + offsetFileName);
                    return;
                }
                lastReadOffsetInfo = Optional.absent();
            } else {
                String offsetConfigs = getOffset();
                if (offsetConfigs == null || offsetConfigs.isEmpty()) {
                    logger.warn("offset config file :" + offsetFileName
                            + " configs error. must be<fileName$fileInode$offset$time>. will ignore this config.");
                    lastReadOffsetInfo = Optional.absent();
                } else {
                    try {
                        lastReadOffsetInfo = Optional.of(new OffsetInfo(null, -1, -1, -1));
                        lastReadOffsetInfo.get().setByString(offsetConfigs);
                    } catch (OffsetInfoException e) {
                        logger.error("set lastReadOffsetInfo by string exception:" + e.getMessage());
                        return;
                    }
                }
            }
        } else {
            lastReadOffsetInfo = Optional.absent();
        }

        try {
            reader = new ReliableTailSourceEventReaderBuilder()
                    .spoolDirectory(directory)
                    .offsetDirectory(offsetDirectory)
                    .ignorePattern(ignorePattern)
                    .ignoreContentPattern(ignoreContentPattern)
                    .filterContentPattern(filterContentPattern)
                    .inputCharset(inputCharset)
                    .decodeErrorPolicy(decodeErrorPolicy)
                    .build();
        } catch (IOException e) {
            throw new FlumeException("Error instantiating spooling event parser" + e.getMessage());
        }

        POLL_DELAY_MS = intervalMillis;
        Runnable runner = new ReliableTailDirRunnable(reader, sourceCounter);
        executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

        logger.info("config ignore content pattern:" + ignoreContentPattern
                + " filter pattern:" + filterContentPattern);

        super.start();
        logger.info("ReliableTailSource started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }
        executor.shutdownNow();
        super.stop();
        sourceCounter.stop();
        logger.info("SpoolDir source " + getName() + " stopped. Metrics:" + sourceCounter);
    }

    @Override
    public String toString() {
        return "Spool directory source:" + getName() + " spoolDir:" + spoolDirectory;
    }

    @Override
    public synchronized void configure(Context context) {

        spoolDirectory = context.getString(ReliableTailSourceConfigurationConstants.SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");

        offsetDirectory = context.getString(ReliableTailSourceConfigurationConstants.OFFSET_DIRPATH_KEY);
        Preconditions.checkState(spoolDirectory != null, "Configuration must specify a offset config directory");

        batchSize = context.getInteger(ReliableTailSourceConfigurationConstants.BATCH_SIZE,
                ReliableTailSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
        inputCharset = context.getString(ReliableTailSourceConfigurationConstants.INPUT_CHARSET,
                ReliableTailSourceConfigurationConstants.DEFAULT_INPUT_CHARSET);
        decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                context.getString(ReliableTailSourceConfigurationConstants.DECODE_ERROR_POLICY,
                        ReliableTailSourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY)
                        .toUpperCase(Locale.ENGLISH));

        ignorePattern = context.getString(ReliableTailSourceConfigurationConstants.IGNORE_PAT,
                ReliableTailSourceConfigurationConstants.DEFAULT_IGNORE_PAT);

        filterContentPattern = context.getString(ReliableTailSourceConfigurationConstants.FILTER_CONTENT_PAT,
                ReliableTailSourceConfigurationConstants.DEFAULT_FILTER_CONTENT_PAT);

        ignoreContentPattern = context.getString(ReliableTailSourceConfigurationConstants.IGNORE_CONTENT_PAT,
                ReliableTailSourceConfigurationConstants.DEFAULT_IGNORE_CONTENT_PAT);

        maxBackoff = context.getInteger(ReliableTailSourceConfigurationConstants.MAX_BACKOFF,
                ReliableTailSourceConfigurationConstants.DEFAULT_MAX_BACKOFF);

        intervalMillis = context.getInteger(ReliableTailSourceConfigurationConstants.INTERVAL_MILLIS,
                ReliableTailSourceConfigurationConstants.DEFAULT_INTERVAL_MILLIS);

        String saveOrNot = context.getString(ReliableTailSourceConfigurationConstants.SAVE_OFFSET_OR_NOT);
        if (saveOrNot == null) {
            saveOffsetOrNot = ReliableTailSourceConfigurationConstants.DEFAULT_SAVE_OFFSET_OR_NOT;
        } else {
            Preconditions.checkState(saveOrNot.equals("true") || saveOrNot.equals("false"),
                    "Configuration must specify saveOffsetOrNot to true or false");
            if (saveOrNot.equals("true")) {
                saveOffsetOrNot = true;
            } else if (saveOrNot.equals("false")) {
                saveOffsetOrNot = false;
            }
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    private class ReliableTailDirRunnable implements Runnable {

        private ReliableTailSourceEventReader reader;
        private SourceCounter sourceCounter;

        public ReliableTailDirRunnable(ReliableTailSourceEventReader reader, SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        @Override
        public void run() {
            int backoffInterval = 100;
            try {
                while (!Thread.interrupted()) {
                    List<Event> events = Lists.newLinkedList();
                    Optional<OffsetInfo> returnOffset =
                            reader.readEvents(batchSize, lastReadOffsetInfo, events);
                    if (events == null) {
                        logger.info("read events is null");
                        break;
                    }
                    if (events.isEmpty()) {
                        if (returnOffset.isPresent()) { // 配置为保存offset
                            // 如果旧文件最后读取的events为空，切换为新文件，会进入该分支
                            if (saveOffsetOrNot) {
                                String saveOffsetString = returnOffset.get().getOffsetString();
                                try {
                                    setOffset(saveOffsetString);
                                } catch (IOException e) {
                                    logger.error("save offset exception." + e.getMessage()
                                            + " offset string:" + saveOffsetString);
                                    break;
                                }
                            }
                            lastReadOffsetInfo = returnOffset;
                        }
                        emptyEventsCount++;
                        // 如果连续读取空消息5次以上，开始延时读，直至下次读取到消息
                        if (emptyEventsCount > 5) {
                            TimeUnit.MILLISECONDS.sleep(emptyEventsDelay);
                            emptyEventsDelay = emptyEventsDelay << 1;
                            emptyEventsDelay =
                                    emptyEventsDelay >= maxEmptyEventDelay ? maxEmptyEventDelay : emptyEventsDelay;
                        }
                        break;
                    }
                    emptyEventsCount = 0;
                    emptyEventsDelay = 100;
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();
                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                        // save offset here
                        if (!returnOffset.isPresent()) {
                            logger.error("get offset info falied.");
                            break;
                        }
                        String saveOffsetString = returnOffset.get().getOffsetString();
                        if (saveOffsetOrNot) { //保存offset
                            try {
                                setOffset(saveOffsetString);
                            } catch (IOException e) {
                                logger.error("save offset exception." + e.getMessage()
                                        + " offset string:" + saveOffsetString);
                                break;
                            }
                        }
                        lastReadOffsetInfo = returnOffset;
                    } catch (ChannelException e) {
                        logger.warn("The channel is full, and cannot write data now. The "
                                + "source will try again after " + String.valueOf(backoffInterval) + " milliseconds");
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff : backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 100;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
            } catch (Throwable e) {
                logger.error("FATAL: " + ReliableTailSource.this.toString() + ": "
                        + "Uncaught exception in ReliableTailSource thread. ");
                // Throwables.propagate(e);
            }
        }
    }

    /**
     * get offset from offset config file
     * @return offset string
     */
    public String getOffset() {
        // load offset info
        String offsetInfo = null;
        File file = new File(offsetDirectory + "/offset");
        if (!file.exists()) {
            logger.error("offset config file:" + file.getName() + " not exist!");
            return null;
        }
        if (!file.canRead()) {
            logger.error("offset config file:" + file.getName() + " cannot read.");
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            offsetInfo = reader.readLine();
            reader.close();
            if (offsetInfo == null) {
                return null;
            }
        } catch (IOException e) {
            logger.error("exception:" + e.getMessage());
            return null;
        }
        return offsetInfo;
    }

    /**
     * save offset to offset config file
     * @param offset offset string
     * @return save success return true; save failed return false
     */
    public void setOffset(String offset) throws IOException {
        File file = new File(offsetDirectory + "/offset");
        if (!file.exists()) {
            file.createNewFile();
        }
        if (!file.canWrite()) {
            logger.error("offset file cannot write.");
            throw new IOException("offset file cannot write.");
        }
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("");
        fileWriter.write(offset);
        fileWriter.close();
    }
}
