package bufmgr;

import global.*;

class FrameDesc {
	boolean dirty;
	boolean valid;
	PageId pageNo;
	int pinCount;
	Page page;
	
	public FrameDesc() {
		dirty = false;
		valid = false;
		pageNo = new PageId(); //INVALID_PAGEID
		pinCount = 0;
		page = null;
	}
}