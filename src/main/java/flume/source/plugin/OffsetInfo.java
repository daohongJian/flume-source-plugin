package flume.source.plugin;

/**
 * Created by jiandaohong on 2015/9/23.
 */

public class OffsetInfo {
    private String fileName = null;
    private int inode = 0;
    private long offset = 0;
    private long modifiedTime = 0;

    // forbid default value
    private OffsetInfo() { }

    public OffsetInfo(String fileName, int inode, long offset, long modifiedTime) {
        this.fileName = fileName;
        this.inode = inode;
        this.offset = offset;
        this.modifiedTime = modifiedTime;
    }

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public int getInode() { return inode; }
    public void setInode(int inode) { this.inode = inode; }
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public long getModifiedTime() { return modifiedTime; }
    public void setModifiedTime(long modifiedTime) { this.modifiedTime = modifiedTime; }

    public String getOffsetString() {
        return fileName + "$" + inode + "$" + offset + "$" + modifiedTime;
    }

    public void setByString(String offsetString) throws OffsetInfoException {
        if (offsetString == null) {
            throw new OffsetInfoException("offsetString is null");
        }
        String[] configs = offsetString.split("\\$");
        if (configs.length != 4) {
            throw new OffsetInfoException("offset string:" + offsetString
                    + " format error.must be <fileName$inode$offset$modifiedTime>");
        }
        this.fileName = configs[0];
        try {
            this.inode = Integer.parseInt(configs[1]);
            this.offset = Long.parseLong(configs[2]);
            this.modifiedTime = Long.parseLong(configs[3]);
        } catch (NumberFormatException e) {
            throw new OffsetInfoException("offset string numberFormatException:" + e.getMessage());
        }
    }
}


