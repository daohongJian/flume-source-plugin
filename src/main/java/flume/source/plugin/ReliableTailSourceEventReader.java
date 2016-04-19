package flume.source.plugin;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jiandaohong on 2015/9/21.
 */

public class ReliableTailSourceEventReader implements ReliableEventReader {

    private static final Logger logger = LoggerFactory
            .getLogger(ReliableTailSourceEventReader.class);

    private final File spoolDirectory;                 // spool dir
    private final Pattern ignorePattern;               // ignore pattern
    private final Pattern ignoreContentPattern;
    private final Pattern filterContentPattern;

    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;

    private final Charset outputCharset = Charset.forName("UTF-8");

    private Optional<FileInfo> currentFile = Optional.absent();
    private Optional<FileInfo> newerFile = Optional.absent();
    /** Always contains the last file from which lines have been read. **/
    private boolean committed = true;

    private SpoolingDirManager spoolingDirManager = null;

    /**
     * Create a ReliableTailSourceEventReader to watch the given directory.
     */

    public ReliableTailSourceEventReader(File spoolDirectory,
                                         String offsetDirectory,
                                         String ignorePattern,
                                         String ignoreContentPattern,
                                         String filterContentPattern,
                                         String inputCharset,
                                         DecodeErrorPolicy decodeErrorPolicy) throws IOException {

        // Sanity checks
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(offsetDirectory);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(ignoreContentPattern);
        Preconditions.checkNotNull(filterContentPattern);
        Preconditions.checkNotNull(inputCharset);

        logger.info("Initializing {} with directory={}," ,
                new Object[] {ReliableTailSourceEventReader .class.getSimpleName(), spoolDirectory});

        // Verify directory exists and is readable/writable
        Preconditions.checkState(spoolDirectory.exists(),
                "Directory does not exist: " + spoolDirectory.getAbsolutePath());
        Preconditions.checkState(spoolDirectory.isDirectory(),
                "Path is not a directory: " + spoolDirectory.getAbsolutePath());

        // Do a canary test to make sure we have access to spooling directory
        try {
            File canary = File.createTempFile("flume-spooldir-perm-check-", ".canary",
                    spoolDirectory);
            Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
            List<String> lines = Files.readLines(canary, Charsets.UTF_8);
            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
            if (!canary.delete()) {
                throw new IOException("Unable to delete canary file " + canary);
            }
            logger.debug("Successfully created and deleted canary file: {}", canary);
        } catch (IOException e) {
            throw new FlumeException("Unable to read and modify files"
                    + " in the spooling directory: " + spoolDirectory, e);
        }

        this.spoolDirectory = spoolDirectory;
        this.ignorePattern = Pattern.compile(ignorePattern);
        this.ignoreContentPattern = Pattern.compile(ignoreContentPattern);
        this.filterContentPattern = Pattern.compile(filterContentPattern);
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
        this.spoolingDirManager = new SpoolingDirManager(this.spoolDirectory, this.ignorePattern);
    }

    @Deprecated
    public Event readEvent() {
        return null;
    }

    @Deprecated
    public List<Event> readEvents(int numEvents) {
        return Lists.newLinkedList();
    }

    /**
     * main read function.will read fixed num of lines from fileName ,
     * put the read events in eventList and return the offset string
     * @param numEvents num to read
     * @param lastReadOffsetInfo offset info
     * @param events Event list to store read values
     * @return offset String
     * @throws IOException
     */

    public Optional<OffsetInfo> readEvents(int numEvents,
                                           final Optional<OffsetInfo> lastReadOffsetInfo,
                                           List<Event> events) throws IOException {

        if (events == null) {
            logger.error("readEvents function: the eventList is null");
            return Optional.absent();
        }
        if (!committed) {
            // 上次提交Channel未成功，则返回
            logger.info("Last read was never committed. will continue;");
            return Optional.absent();
        }
        String fileName = null;
        long offset = -1;
        long time = -1;
        int inode = -1;
        if (lastReadOffsetInfo.isPresent()) {
            fileName = lastReadOffsetInfo.get().getFileName();
            offset = lastReadOffsetInfo.get().getOffset();
            time = lastReadOffsetInfo.get().getModifiedTime();
            inode = lastReadOffsetInfo.get().getInode();
        }

        // 第一次启动flume，或者offset配置文件丢失，会进入该分支
        if (!lastReadOffsetInfo.isPresent()) {
            // 选择当前文件夹最新的文件
            currentFile = spoolingDirManager.getNewestFile();
            if (!currentFile.isPresent()) {
                logger.info("there is no file to read.");
                return Optional.absent();
            }
            time = currentFile.get().getLastModified();
            fileName = currentFile.get().getFile().getName();
            offset = currentFile.get().resetBufferedReaderToEndOfFile();
            inode = spoolingDirManager.getFileInode(fileName);
            logger.info("first time run.will get the newest file to read. file name:" + fileName
                + ". modify time:" + time);
        }
        // flume重启（offset配置文件存在），或者重置currentFile（inode改变）会进入该分支
        if (!currentFile.isPresent()) {
            // 首先会根据inode查找文件
            // 对于日志文件，名称可能会改变，因此根据inode和modifiedTime查找
            currentFile = spoolingDirManager.getFileByInode(inode);
            if (currentFile.isPresent()) {
                // 根据inode找到文件后，重置fileName和time
                fileName = currentFile.get().getFile().getName();
                time = currentFile.get().getLastModified();
                // 重置reader的offset
                currentFile.get().resetBufferedReader(offset);
                logger.info("get the file by inode success. fileName:" + fileName);
            } else {
                // 如果根据inode未找到文件，则根据modifiedTime查找，
                // 找到比该时间新(必须包含该时间,如果改文件为最新)的文件中最老的文件开始读取
                currentFile = spoolingDirManager.getNextFile(time);
                if (currentFile.isPresent()) {
                    String curFileName = currentFile.get().getFile().getName();
                    logger.info("get a oldest file which modifiedTime later than " + time
                            + " file name:" + curFileName);
                    long curFileTime = currentFile.get().getLastModified();
                    // 为确保重启后找到的文件是否是原文件（原读取的文件inode改变的影响），根据文件名继续判断
                    if (fileName.equals(curFileName)) {
                        // 如果文件名相同，我们可以认为是同一个文件（存在不确定性）
                        logger.info("fileName:" + fileName + " ModifyTime:" + time
                                + " matched. will read from file:" + curFileName);
                    } else {
                        // 如果根据时间查找的文件名字不是以前的名字，为防可能丢失数据，将offset置0，继续读取
                        offset = 0;
                        logger.info("fileName:" + fileName + "ModifyTime:" + time + "do not match. "
                                + "will read from file:" + curFileName);
                    }
                    // 重置Reader的offset
                    currentFile.get().resetBufferedReader(offset);
                    // 再重置fileName，inode，time
                    fileName = curFileName;
                    time = curFileTime;
                    inode = spoolingDirManager.getFileInode(fileName);
                } else {
                    logger.info("cannot find a file which modifiedTime later than " + time + "will continue.");
                }
            }
        }
        if (currentFile.isPresent()) {
            // 根据inode检查文件是否改变（日志文件被mv后，同名文件inode会改变），inode改变后，重置并返回
            String curFileName = currentFile.get().getFile().getName();
            int newInode = spoolingDirManager.getFileInode(curFileName);
            if (newInode != inode) {
                logger.info("file :" + fileName + " inode has changed.will reset and continue.");
                currentFile = Optional.absent();
                return Optional.absent();
            }

            BufferedReader reader = currentFile.get().getBufferedReader();
            if (reader == null) {
                logger.info("reader is null. will reset BufferedReader .offset:" + offset);
                currentFile.get().resetBufferedReader(offset);
            }
            // 如果inode未改变，判断该文件是否是文件夹中最新的文件
            newerFile = spoolingDirManager.getNewerFile(time);
            // 当前文件是最新的，或者是虽然不是最新的文件，但名字和查找的最新文件名相同
            // （实时日志文件 modifiedTime会一直改变，也会被判断有新文件，实际为同一个文件）
            if (!newerFile.isPresent() || newerFile.get().getFile().getName().equals(curFileName)) {
                if (newerFile.isPresent()) {
                    logger.info("find a newer file. file name:" + newerFile.get().getFile().getName());
                }
                logger.info("current read file is the newest file.file name:" + curFileName);
                String line;
                while (numEvents-- > 0) {
                    line = reader.readLine();
                    if (line == null) {
                        logger.info("arrive end of file:" + curFileName + " read offset:" + offset);
                        break;
                    }
                    offset++;
                    if (!filterContentPattern.matcher(line).matches()) {
                        continue;
                    }
                    if (ignoreContentPattern.matcher(line).matches()) {
                        continue;
                    }
                    Event event = EventBuilder.withBody(line, outputCharset);
                    events.add(event);
                }
                time = currentFile.get().getLastModified();
            } else { // 当前文件不是最新的文件
                logger.info("current read file is not the newest file.");
                String newFileName = newerFile.get().getFile().getName();
                while (numEvents-- > 0) {
                    String line = reader.readLine();
                    if (line == null) {
                        // 当前文件不是最新文件，而且已经全部读完，则关闭该文件的Reader，并切换成较新的文件继续读取
                        currentFile.get().getBufferedReader().close();
                        currentFile = newerFile;
                        logger.info("current file has read finish. will close and read the next file:" + newFileName);
                        // 重置新文件相关offset信息
                        fileName = currentFile.get().getFile().getName();
                        time = currentFile.get().getLastModified();
                        inode = spoolingDirManager.getFileInode(fileName);
                        offset = 0;
                        return Optional.of(new OffsetInfo(fileName, inode, offset, time));
                    }
                    offset++;
                    if (!filterContentPattern.matcher(line).matches()) {
                        continue;
                    }
                    if (ignoreContentPattern.matcher(line).matches()) {
                        continue;
                    }
                    Event event = EventBuilder.withBody(line, outputCharset);
                    events.add(event);
                }
                time = currentFile.get().getLastModified();
            }
            logger.info("reading file:" + fileName + " read lines:" + events.size());
        }
        if (events.size() != 0) {
            // only non-empty events need to commit
            committed = false;
        }
        return Optional.of(new OffsetInfo(fileName, inode, offset, time));
    }

    @Override
    public void close() throws IOException {
        if (currentFile.isPresent()) {
            currentFile.get().getBufferedReader().close();
            currentFile = Optional.absent();
        }
    }

    /** Commit the last lines which were read. */
    @Override
    public void commit() throws IOException {
        if (!committed && currentFile.isPresent()) {
            committed = true;
        }
    }
}