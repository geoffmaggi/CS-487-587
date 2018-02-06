/* Minibase Implementation for CS 487/587
 * Docs: http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/minibase.html
 * Authors: Alexander Goddard & Geoff Maggi
 */

package bufmgr;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3> The buffer manager manages an array of main
 * memory pages. The array is called the buffer pool, each page is called a
 * frame. It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and relational
 * operators.
 */
public class BufMgr implements GlobalConst {
	protected FrameDesc[] bufPool;
	protected HashMap<Integer, FrameDesc> bufMap;
	public Clock replPolicy;

	/**
	 * Constructs a buffer manager by initializing member data.
	 * 
	 * @param numframes
	 *          number of frames in the buffer pool
	 */
	public BufMgr(int numframes) {
		bufPool = new FrameDesc[numframes];
		for (int i = 0; i < numframes; i++) {
			bufPool[i] = new FrameDesc();
		}
		bufMap = new HashMap<>();
		replPolicy = new Clock();

		// throw new UnsupportedOperationException("Not implemented");
	} // public BufMgr(int numframes)

	/**
	 * The result of this call is that disk page number pageno should reside in a
	 * frame in the buffer pool and have an additional pin assigned to it, and
	 * mempage should refer to the contents of that frame. <br>
	 * <br>
	 * 
	 * If disk page pageno is already in the buffer pool, this simply increments the
	 * pin count. Otherwise, this<br>
	 * 
	 * <pre>
	 * 	uses the replacement policy to select a frame to replace
	 * 	writes the frame's contents to disk if valid and dirty
	 * 	if (contents == PIN_DISKIO)
	 * 		read disk page pageno into chosen frame
	 * 	else (contents == PIN_MEMCPY)
	 * 		copy mempage into chosen frame
	 * 	[omitted from the above is maintenance of the frame table and hash map]
	 * </pre>
	 * 
	 * @param pageno
	 *          identifies the page to pin
	 * @param mempage
	 *          An output parameter referring to the chosen frame. If
	 *          contents==PIN_MEMCPY it is also an input parameter which is copied
	 *          into the chosen frame, see the contents parameter.
	 * @param contents
	 *          Describes how the contents of the frame are determined.<br>
	 *          If PIN_DISKIO, read the page from disk into the frame.<br>
	 *          If PIN_MEMCPY, copy mempage into the frame.<br>
	 *          If PIN_NOOP, copy nothing into the frame - the frame contents are
	 *          irrelevant.<br>
	 *          Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
	 * @throws IllegalArgumentException
	 *           if PIN_MEMCPY and the page is pinned.
	 * @throws IllegalStateException
	 *           if all pages are pinned (i.e. pool is full)
	 */
	public void pinPage(PageId pageno, Page mempage, int contents) {
		if(bufMap.containsKey(pageno.pid) && bufMap.get(pageno.pid).valid()) {
			bufMap.get(pageno.pid).pinCount++;
			return;
		}
		
		if (getNumUnpinned() == 0) {
			throw new IllegalStateException("The buffer pool is full");
		}
		
		int frameno = replPolicy.pickVictim(bufPool);
		
		if(bufPool[frameno].valid() && bufPool[frameno].dirty) {
			flushPage(bufPool[frameno].pageNo);
		}
		
		int data = 0;
		data = Convert.getIntValue(0, mempage.getData());
		
		bufMap.remove(bufPool[frameno].pageNo.pid);
		switch (contents) {
			case PIN_DISKIO:
				Minibase.DiskManager.read_page(pageno, mempage);
				bufPool[frameno] = new FrameDesc(false, true, pageno, 1, mempage);
				bufMap.put(pageno.pid, bufPool[frameno]);
				
				data = Convert.getIntValue(0, mempage.getData());
				break;
			case PIN_MEMCPY:
				if (this.bufMap.containsKey(pageno.pid) && this.bufMap.get(pageno.pid).pinCount > 0) {
					throw new IllegalArgumentException("Page: " + pageno + " is pinned.");
				}
				this.bufPool[frameno] = new FrameDesc(false, true, pageno, 1, mempage);
				this.bufMap.put(pageno.pid, this.bufPool[frameno]);
				break;
			case PIN_NOOP:
				this.bufPool[frameno] = new FrameDesc(false, true, pageno, 1, mempage);
				this.bufMap.put(pageno.pid, this.bufPool[frameno]);
				break;
		}

	} // public void pinPage(PageId pageno, Page page, int contents)

	/**
	 * Unpins a disk page from the buffer pool, decreasing its pin count.
	 * 
	 * @param pageno
	 *          identifies the page to unpin
	 * @param dirty
	 *          UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
	 * @throws IllegalArgumentException
	 *           if the page is not in the buffer pool or not pinned
	 */
	public void unpinPage(PageId pageno, boolean dirty) {
		if (!this.bufMap.containsKey(pageno.pid)) {
			throw new IllegalArgumentException("Page: " + pageno + " not found");
		}
		if (!this.bufMap.containsKey(pageno.pid) || this.bufMap.get(pageno.pid).pinCount == 0) {
			throw new IllegalArgumentException("Page: " + pageno + " is not pinned");
		}
		if(dirty == UNPIN_DIRTY) {
			this.bufMap.get(pageno.pid).dirty = true;
			flushPage(pageno);
		}
		
		this.bufMap.get(pageno.pid).pinCount -= 1;
	} // public void unpinPage(PageId pageno, boolean dirty)

	/**
	 * Allocates a run of new disk pages and pins the first one in the buffer pool.
	 * The pin will be made using PIN_MEMCPY. Watch out for disk page leaks.
	 * 
	 * @param firstpg
	 *          input and output: holds the contents of the first allocated page and
	 *          refers to the frame where it resides
	 * @param run_size
	 *          input: number of pages to allocate
	 * @return page id of the first allocated page
	 * @throws IllegalArgumentException
	 *           if firstpg is already pinned
	 * @throws IllegalStateException
	 *           if all pages are pinned (i.e. pool exceeded)
	 */
	public PageId newPage(Page firstpg, int run_size) {
		if (getNumUnpinned() == 0) {
			throw new IllegalStateException("All pages are pinned");
		}

		PageId pid = Minibase.DiskManager.allocate_page(run_size);
		if (bufMap.containsKey(pid.pid) && bufMap.get(pid.pid).pinCount > 0) {
			throw new IllegalArgumentException("firstpg(" + pid + ") is already pinned");
		}
		pinPage(pid, firstpg, PIN_MEMCPY);
		return pid;

		// throw new UnsupportedOperationException("Not implemented");
	} // public PageId newPage(Page firstpg, int run_size)

	/**
	 * Deallocates a single page from disk, freeing it from the pool if needed.
	 * 
	 * @param pageno
	 *          identifies the page to remove
	 * @throws IllegalArgumentException
	 *           if the page is pinned
	 */
	public void freePage(PageId pageno) {
		if (bufMap.containsKey(pageno.pid)) {
			if (bufMap.get(pageno.pid).pinCount > 0) {
				throw new IllegalArgumentException(pageno + " is pinned");
			}
	
			bufMap.get(pageno.pid).valid = false;
			bufMap.remove(pageno.pid);
		}
		
		Minibase.DiskManager.deallocate_page(pageno);
		// throw new UnsupportedOperationException("Not implemented");
	} // public void freePage(PageId firstid)

	/**
	 * Write all valid and dirty frames to disk. Note flushing involves only
	 * writing, not unpinning or freeing or the like.
	 * 
	 */
	public void flushAllFrames() {
		for (int i = 0; i < bufPool.length; i++) {
			if (bufPool[i].valid()) {
				flushPage(bufPool[i].pageNo);
			}
		}
		// throw new UnsupportedOperationException("Not implemented");
	} // public void flushAllFrames()

	/**
	 * Write a page in the buffer pool to disk, if dirty.
	 * 
	 * @throws IllegalArgumentException
	 *           if the page is not in the buffer pool
	 */
	public void flushPage(PageId pageno) {
		if (!bufMap.containsKey(pageno.pid)) {
			throw new IllegalArgumentException(pageno + " is not in memory");
		}
		if (bufMap.get(pageno.pid).dirty) {
			Minibase.DiskManager.write_page(pageno, bufMap.get(pageno.pid).page);
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the total number of buffer frames.
	 */
	public int getNumFrames() {
		return bufPool.length;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {
		int count = 0;
		for (int i = 0; i < bufPool.length; i++) {
			if (bufPool[i].pinCount == 0) {
				count++;
			}
		}
		return count;
		// throw new UnsupportedOperationException("Not implemented");
	}

} // public class BufMgr implements GlobalConst
