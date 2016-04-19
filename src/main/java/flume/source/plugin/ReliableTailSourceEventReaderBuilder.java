package flume.source.plugin;

import org.apache.flume.serialization.DecodeErrorPolicy;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

/**
 * Created by jiandaohong on 2015/9/25.
 */
public class ReliableTailSourceEventReaderBuilder {
    /**
     * Special builder class for ReliableTailSourceEventReader
     */
    private File spoolDirectory;
    private String offsetDirectory;
    private String ignorePattern =
            ReliableTailSourceConfigurationConstants.DEFAULT_IGNORE_PAT;
    private String inputCharset =
            ReliableTailSourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
    private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
            ReliableTailSourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY
                    .toUpperCase(Locale.ENGLISH));
    private String filterContentPattern =
            ReliableTailSourceConfigurationConstants.DEFAULT_FILTER_CONTENT_PAT;
    private String ignoreContentPattern =
            ReliableTailSourceConfigurationConstants.DEFAULT_IGNORE_CONTENT_PAT;

    public ReliableTailSourceEventReaderBuilder spoolDirectory(File directory) {
        this.spoolDirectory = directory;
        return this;
    }

    public ReliableTailSourceEventReaderBuilder offsetDirectory(String offsetDirectory) {
        this.offsetDirectory = offsetDirectory;
        return this;
    }

    public ReliableTailSourceEventReaderBuilder ignorePattern(String ignorePattern) {
        this.ignorePattern = ignorePattern;
        return this;
    }

    public ReliableTailSourceEventReaderBuilder ignoreContentPattern(String ignoreContentPattern) {
        this.ignoreContentPattern = ignoreContentPattern;
        return this;
    }

    public ReliableTailSourceEventReaderBuilder filterContentPattern(String filterContentPattern) {
        this.filterContentPattern = filterContentPattern;
        return this;
    }

    public ReliableTailSourceEventReaderBuilder inputCharset(String inputCharset) {
        this.inputCharset = inputCharset;
        return this;
    }

    public ReliableTailSourceEventReaderBuilder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
        this.decodeErrorPolicy = decodeErrorPolicy;
        return this;
    }

    public ReliableTailSourceEventReader build() throws IOException {
        return new ReliableTailSourceEventReader(spoolDirectory,
                offsetDirectory,
                ignorePattern,
                ignoreContentPattern,
                filterContentPattern,
                inputCharset,
                decodeErrorPolicy);
    }
}
