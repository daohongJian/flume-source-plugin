package flume.source.plugin;

import org.apache.flume.serialization.DecodeErrorPolicy;

/**
 * Created by jiandaohong on 2015/9/21.
 */

public class ReliableTailSourceConfigurationConstants {
    // 监控的日志文件夹路径
    public static final String SPOOL_DIRECTORY = "spoolDir";

    // 保存offset信息的文件夹路径
    public static final String OFFSET_DIRPATH_KEY = "offsetDir";

    // 是否保存offset
    public static final String SAVE_OFFSET_OR_NOT = "saveOffsetOrNot";
    public static final boolean DEFAULT_SAVE_OFFSET_OR_NOT = false;

    // 一次读取的大小
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    // 抓取间隔时间
    public static final String INTERVAL_MILLIS = "intervalMillis";
    public static final int DEFAULT_INTERVAL_MILLIS = 300;

    // 忽略的文件名的正则表达式匹配
    public static final String IGNORE_PAT = "ignoreFilePattern";
    public static final String DEFAULT_IGNORE_PAT = "^$"; // no effect

    // 忽略正则表达式匹配
    public static final String IGNORE_CONTENT_PAT = "ignoreContentPattern";
    public static final String DEFAULT_IGNORE_CONTENT_PAT = "^$";

    // 过滤正则表达式匹配
    public static final String FILTER_CONTENT_PAT = "filterContentPattern";
    public static final String DEFAULT_FILTER_CONTENT_PAT = ".*";

    /** Character set used when reading the input. */
    public static final String INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

    /** What to do when there is a character set decoding error. */
    public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final String DEFAULT_DECODE_ERROR_POLICY =
            DecodeErrorPolicy.FAIL.name();

    // Channel已满时回滚的最大延时（ms）
    public static final String MAX_BACKOFF = "maxBackoff";
    public static final Integer DEFAULT_MAX_BACKOFF = 4000;
}
