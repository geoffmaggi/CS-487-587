package bufmgr;

import global.*;

class FrameDesc {
	boolean dirty;
	boolean valid;
	boolean refbit;
	PageId pageNo;
	int pinCount;
	Page page;

	public FrameDesc() {
		dirty = false;
		valid = false;
		refbit = false;
		pageNo = new PageId(); // INVALID_PAGEID
		pinCount = 0;
		page = null;
	}

	public FrameDesc(boolean dirty, boolean valid, int pageNo, int pinCount, Page page) {
		this.dirty = dirty;
		this.valid = valid;
		this.refbit = true;
		this.pageNo = new PageId(pageNo);
		this.pinCount = pinCount;
		this.page = page;
	}
	
	public boolean valid() {
		if(!valid || pinCount<1) return false;
		return true;
	}
}