/* Minibase Implementation for CS 487/587
 * Docs: http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/minibase.html
 * Authors: Alexander Goddard & Geoff Maggi
 */

package bufmgr;

import global.Page;
import global.PageId;

class FrameDesc {
	boolean dirty;
	boolean valid;
	boolean refbit;
	PageId pageNo;
	int pinCount;
	Page page;

	/**
	 * Blank frame with new pageID & Page. (It is invalid until you set it)
	 */
	public FrameDesc() {
		dirty = false;
		valid = false;
		refbit = false;
		pageNo = new PageId(); //Invalid at first
		pinCount = 0;
		page = new Page();
	}

	/**
	 * Sets the new ID of the frame and resets dirty, valid, refbit and pin count.
	 * @param pageNo
	 * 			The new Page ID
	 */
	public void setFrame(PageId pageNo) {
		this.dirty = false;
		this.valid = true;
		this.refbit = true;
		this.pageNo.copyPageId(pageNo); //deep copy the pageNo
		this.pinCount = 1;
	}
}