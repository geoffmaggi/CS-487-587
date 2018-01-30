package bufmgr;

import global.*;

class FrameDesc {
	boolean dirty;
	boolean valid;
	PageId pageNo;
	int pinCount;
	
	public FrameDesc() {
		dirty = false;
		valid = false;
		pageNo = new PageId(); //INVALID_PAGEID
		pinCount = 0;
	}
}