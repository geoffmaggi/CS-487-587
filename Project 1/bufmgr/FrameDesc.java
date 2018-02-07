package bufmgr;

import global.PageId;

public class FrameDesc {
    public boolean dirty;
    public boolean valid;
    public boolean refBit;
    public PageId pageNo;
    public int pinCount;
    public int index;

    public FrameDesc(int index) {
        dirty = false;
        valid = false;
        refBit = false;
        pageNo = new PageId();
        pinCount = 0;
        this.index = index;
    }
}
