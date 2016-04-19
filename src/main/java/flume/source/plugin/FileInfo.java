package flume.source.plugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by jiandaohong on 2015/9/25.
 */
public class FileInfo {
    private final File file;
    private long lastModified;
    private BufferedReader bufferedReader;

    public FileInfo(File file) throws FileNotFoundException {
        this.file = file;
        this.lastModified = file.lastModified();
        FileReader fileReader = new FileReader(file);
        this.bufferedReader = new BufferedReader(fileReader);
    }

    public long getLastModified() {
        this.lastModified = file.lastModified();
        return lastModified;
    }
    public BufferedReader getBufferedReader() { return bufferedReader; }
    public File getFile() { return file; }

    public void resetBufferedReader(long offset) throws IOException {
        this.bufferedReader = new BufferedReader(new FileReader(file));
        while (offset-- > 0) {
            if (bufferedReader.readLine() == null) {
                break;
            }
        }
    }

    public int resetBufferedReaderToEndOfFile() throws IOException {
        this.bufferedReader = new BufferedReader(new FileReader(file));
        int offset = 0;
        while (true) {
            if (bufferedReader.readLine() == null) {
                break;
            }
            offset++;
        }
        return offset;
    }
}