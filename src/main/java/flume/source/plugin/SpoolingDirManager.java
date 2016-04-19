package flume.source.plugin;

import com.google.common.base.Optional;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jiandaohong on 2015/9/25.
 */
public class SpoolingDirManager {
    private static final Logger logger = LogManager.getLogger(SpoolingDirManager.class);

    private final File spoolDirectory;
    private final Pattern ignorePattern;
    /** Instance var to Cache directory listing **/
    private Iterator<File> candidateFileIter = null;

    private SpoolingDirManager() {
        spoolDirectory = null;
        ignorePattern = null;
    }

    public SpoolingDirManager(File spoolDirectory, Pattern ignorePattern) {
        this.spoolDirectory = spoolDirectory;
        this.ignorePattern = ignorePattern;
    }

    public Optional<FileInfo> getFileByInode(int inode) {
        FileFilter filter = new FileFilter() {
            public boolean accept(File candidate) {
                String fileName = candidate.getName();
                if ((candidate.isDirectory())
                        || (fileName.startsWith("."))
                        || ignorePattern.matcher(fileName).matches()) {
                    return false;
                }
                return true;
            }
        };
        List<File> fileList = Arrays.asList(spoolDirectory.listFiles(filter));
        Iterator<File> iterator = fileList.iterator();
        // No matching file in spooling directory.
        if (!iterator.hasNext()) {
            return Optional.absent();
        }
        File selectedFile = null;
        int tempNode;
        for (File candidateFile : fileList) {
            tempNode = getFileInode(candidateFile.getName());
            if (tempNode == -1) {
                logger.warn("get file inode failed.");
                continue;
            }
            if (tempNode == inode) {
                selectedFile = candidateFile;
                break;
            }
        }
        // no match file exist
        if (selectedFile == null) {
            return Optional.absent();
        }
        return openFile(selectedFile);
    }

    /**
     * this function will just run for the first time to run flume(or the offset file not exist or deleted)
     * get the newest file and read from this file
     * @return
     */
    public Optional<FileInfo> getNewestFile() {
        FileFilter filter = new FileFilter() {
            public boolean accept(File candidate) {
                String fileName = candidate.getName();
                if ((candidate.isDirectory())
                        || (fileName.startsWith("."))
                        || ignorePattern.matcher(fileName).matches()) {
                    return false;
                }
                return true;
            }
        };
        List<File> fileList = Arrays.asList(spoolDirectory.listFiles(filter));
        Iterator<File> iterator = fileList.iterator();
        // No matching file in spooling directory.
        if (!iterator.hasNext()) {
            return Optional.absent();
        }
        // No matching file in spooling directory.
        if (!iterator.hasNext()) {
            return Optional.absent();
        }
        File selectedFile = iterator.next();
        for (File candidateFile : fileList) {
            long compare = selectedFile.lastModified() - candidateFile.lastModified();
            if (compare == 0) { // ts is same pick smallest lexicographically.
                selectedFile = smallerLexicographical(selectedFile, candidateFile);
            } else if (compare < 0) { // candidate is newer (cand-ts < selec-ts)
                selectedFile = candidateFile;
            }
        }
        return openFile(selectedFile);
    }

    public Optional<FileInfo> getNewerFile(final long lastReadTime) {
        List<File> candidateFiles = Collections.emptyList();
        /* Filter to exclude finished or hidden files */
        FileFilter filter = new FileFilter() {
            public boolean accept(File candidate) {
                String fileName = candidate.getName();
                if ((candidate.isDirectory())
                        || (fileName.startsWith("."))
                        || ignorePattern.matcher(fileName).matches()
                        || (candidate.lastModified() <= lastReadTime)) {
                    return false;
                }
                return true;
            }
        };
        candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
        candidateFileIter = candidateFiles.iterator();

        // No matching file in spooling directory.
        if (!candidateFileIter.hasNext()) {
            return Optional.absent();
        }

        File selectedFile = candidateFileIter.next();
        for (File candidateFile : candidateFiles) {
            long compare = selectedFile.lastModified() - candidateFile.lastModified();
            if (compare == 0) { // ts is same pick smallest lexicographically.
                selectedFile = smallerLexicographical(selectedFile, candidateFile);
            } else if (compare > 0) { // candidate is older (cand-ts < selec-ts)
                selectedFile = candidateFile;
            }
        }
        return openFile(selectedFile);
    }
    /**
     * Returns the next file to be consumed from the chosen directory.
     * If the directory is empty or the chosen file is not readable,
     * this will return an absent option.
     * the chosen file's modify time must be above lastReadTime
     */
    public Optional<FileInfo> getNextFile(final long lastReadTime) {
        List<File> candidateFiles = Collections.emptyList();
        /* Filter to exclude finished or hidden files */
        FileFilter filter = new FileFilter() {
            public boolean accept(File candidate) {
                String fileName = candidate.getName();
                if ((candidate.isDirectory())
                        || (fileName.startsWith("."))
                        || ignorePattern.matcher(fileName).matches()
                        || (candidate.lastModified() < lastReadTime)) {
                    return false;
                }
                return true;
            }
        };
        candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
        candidateFileIter = candidateFiles.iterator();

        // No matching file in spooling directory.
        if (!candidateFileIter.hasNext()) {
            return Optional.absent();
        }

        File selectedFile = candidateFileIter.next();
        for (File candidateFile : candidateFiles) {
            long compare = selectedFile.lastModified() - candidateFile.lastModified();
            if (compare == 0) { // ts is same pick smallest lexicographically.
                selectedFile = smallerLexicographical(selectedFile, candidateFile);
            } else if (compare > 0) { // candidate is older (cand-ts < selec-ts)
                selectedFile = candidateFile;
            }
        }
        return openFile(selectedFile);
    }

    public int getFileInode(String fileName) {
        Path path = Paths.get(spoolDirectory + "/" + fileName);
        try {
            BasicFileAttributes attributes = java.nio.file.Files.readAttributes(path, BasicFileAttributes.class);
            Object key = attributes.fileKey();
            if (key == null) {
                return -1;
            }
            String fileKey = key.toString();
            int index = fileKey.indexOf("ino=");
            index += 4;
            fileKey = fileKey.substring(index);
            int inode = Integer.parseInt(fileKey.replaceAll("\\D+", ""));
            return inode;
        } catch (IOException e) {
            logger.warn("get unique key of file:" + fileName + " exception");
            return -1;
        }
    }

    public File smallerLexicographical(File f1, File f2) {
        if (f1.getName().compareTo(f2.getName()) < 0) {
            return f1;
        }
        return f2;
    }
    /**
     * Opens a file for consuming
     * @param file
     * file does not exists or readable.
     */
    public Optional<FileInfo> openFile(File file) {
        try {
            return Optional.of(new FileInfo(file));
        } catch (FileNotFoundException e) {
            // File could have been deleted in the interim
            logger.warn("Could not find file: " + file, e);
            return Optional.absent();
        } catch (IOException e) {
            logger.error("Exception opening file: " + file, e);
            return Optional.absent();
        }
    }
}
