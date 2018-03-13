/* Minibase Implementation for CS 487/587
 * Docs: http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/minibase.html
 * Authors: Alexander Goddard & Geoff Maggi
 * Github: https://github.com/geoffmaggi/CS-487-587
 */

package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;

/**
 * A HashScan retrieves all records with a given key (via the RIDs of the
 * records). It is created only through the function openScan() in the HashIndex
 * class.
 */
public class HashScan implements GlobalConst {

	/** The search key to scan for. */
	protected SearchKey key;

	/** Id of HashBucketPage being scanned. */
	protected PageId curPageId;

	/** HashBucketPage being scanned. */
	protected HashBucketPage curPage;

	/** Current slot to scan from. */
	protected int curSlot;

	// --------------------------------------------------------------------------

	/**
	 * Constructs an equality scan by initializing the iterator state.
	 */
	protected HashScan(HashIndex index, SearchKey key) {
		int hash = key.getHash(index.DEPTH);
		this.key = new SearchKey(key);

		PageId pageno = new PageId(index.headId.pid);
		HashDirPage dirPage = new HashDirPage();

		while (hash >= HashDirPage.MAX_ENTRIES) {
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);
			PageId nextId = dirPage.getNextPage();
			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno = nextId;
			hash -= HashDirPage.MAX_ENTRIES;
		}

		Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);
		curPageId = dirPage.getPageId(hash);
		Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
		curPage = new HashBucketPage();

		if (curPageId.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(curPageId, curPage, PIN_DISKIO);
			curSlot = EMPTY_SLOT;
		}
	} // protected HashScan(HashIndex index, SearchKey key)

	/**
	 * Called by the garbage collector when there are no more references to the
	 * object; closes the scan if it's still open.
	 */
	protected void finalize() throws Throwable {
		if (curPageId.pid != INVALID_PAGEID) {
			close();
		}
	} // protected void finalize() throws Throwable

	/**
	 * Closes the index scan, releasing any pinned pages.
	 */
	public void close() {
		if (curPageId.pid != INVALID_PAGEID) {
			Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
			curPageId.pid = INVALID_PAGEID;
		}
	} // public void close()

	/**
	 * Gets the next entry's RID in the index scan.
	 * 
	 * @throws IllegalStateException
	 *           if the scan has no more entries
	 */
	public RID getNext() {
		while (curPageId.pid != INVALID_PAGEID) {
			curSlot = curPage.nextEntry(key, curSlot);

			if (curSlot < 0) {
				PageId nextId = curPage.getNextPage();
				Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN); // unpin the old
				curPageId = nextId;
				if (curPageId.pid != INVALID_PAGEID) {
					Minibase.BufferManager.pinPage(curPageId, curPage, PIN_DISKIO); // pin the new
				}
			} else {
				try {
					return curPage.getEntryAt(curSlot).rid;
				} catch (Exception e) {
					throw new IllegalStateException("No More Entries");
				}
			}
		}
		return null; // There is no next
	} // public RID getNext()

} // public class HashScan implements GlobalConst
