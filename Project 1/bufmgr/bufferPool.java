package bufmgr;

import global.*;

class bufferPool {
	FrameDesc[] bufPool;
	
	bufferPool(int size) {
		bufPool = new FrameDesc[size];
	}
	
	//Checks to see if the buffer contains the given pageNo
	boolean contains(PageId pageNo) {
		for(FrameDesc fd: bufPool) {
			if(fd.pageNo.equals(pageNo)) return true;
		}
		return false;
	}
	
	FrameDesc get(PageId pageNo) {
		for(FrameDesc fd: bufPool) {
			if(fd.pageNo.equals(pageNo)) return fd;
		}
		throw new IllegalArgumentException(pageNo + " not in buffer."); //might want to handle this gracefully.
	}
}