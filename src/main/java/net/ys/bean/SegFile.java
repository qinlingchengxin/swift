package net.ys.bean;

/**
 * User: LiWenC
 * Date: 17-12-21
 */
public class SegFile {

    private String tempName;//切分临时文件前缀

    private int index;//切分文件索引号

    private long startPoint;//起始点

    public SegFile(String tempName, int index, long startPoint) {
        this.tempName = tempName;
        this.index = index;
        this.startPoint = startPoint;
    }

    public int getIndex() {
        return index;
    }

    public long getStartPoint() {
        return startPoint;
    }

    public String getTempName() {
        return tempName;
    }
}
