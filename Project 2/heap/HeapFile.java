/* Minibase Implementation for CS 487/587
 * Docs: http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/minibase.html
 * Authors: Alexander Goddard & Geoff Maggi
 */

package heap;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;

/**
 * <h3>Minibase Heap Files</h3> A heap file is the simplest database file
 * structure. It is an unordered set of records, stored on a set of data pages.
 * <br>
 * This class supports inserting, selecting, updating, and deleting records.<br>
 * Normally each heap file has an entry in the database's file library.
 * Temporary heap files are used for external sorting and in other relational
 * operators. A temporary heap file does not have an entry in the file library
 * and is deleted when there are no more references to it. <br>
 * A sequential scan of a heap file (via the HeapScan class) is the most basic
 * access method.
 */
public class HeapFile implements GlobalConst {

	/** HFPage type for directory pages. */
	protected static final short DIR_PAGE = 10;

	/** HFPage type for data pages. */
	protected static final short DATA_PAGE = 11;

	// --------------------------------------------------------------------------

	/** Is this a temporary heap file, meaning it has no entry in the library? */
	protected boolean isTemp;

	/**
	 * The heap file name. Null if a temp file, otherwise used for the file library
	 * entry.
	 */
	protected String fileName;

	/** First page of the directory for this heap file. */
	protected PageId headId;

	// --------------------------------------------------------------------------

	/**
	 * If the given name is in the library, this opens the corresponding heapfile;
	 * otherwise, this creates a new empty heapfile. A null name produces a
	 * temporary file which requires no file library entry.
	 */
	public HeapFile(String name) {
		if (name != null) {
			headId = Minibase.DiskManager.get_file_entry(name); // Attempts to open the corresponding heapfile
			isTemp = false;

			if (headId == null) { // New page
				DirPage newPage = new DirPage();
				headId = Minibase.BufferManager.newPage(newPage, 1);
				newPage.setCurPage(headId);
				Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
				Minibase.DiskManager.add_file_entry(name, headId);
			}
		} else {
			isTemp = true;
		}
	} // public HeapFile(String name)

	/**
	 * Called by the garbage collector when there are no more references to the
	 * object; deletes the heap file if it's temporary.
	 */
	protected void finalize() throws Throwable {
		if (isTemp) {
			deleteFile();
		}
	} // protected void finalize() throws Throwable

	/**
	 * Deletes the heap file from the database, freeing all of its pages and its
	 * library entry if appropriate.
	 */
	public void deleteFile() {
		PageId pageno = new PageId(headId.pid);
		DirPage dirPage = new DirPage();

		// For all pages
		while (pageno.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_NOOP);
			for (int i = 0; i < dirPage.getEntryCnt(); i++) {
				Minibase.BufferManager.freePage(dirPage.getPageId(i));
			}

			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			Minibase.BufferManager.freePage(pageno);
			pageno.copyPageId(dirPage.getNextPage());
		}

		if (!isTemp) {
			Minibase.DiskManager.delete_file_entry(fileName);
		}
	} // public void deleteFile()

	/**
	 * Inserts a new record into the file and returns its RID. Should be efficient
	 * about finding space for the record. However, fixed length records inserted
	 * into an empty file should be inserted sequentially. Should create a new
	 * directory and/or data page only if necessary.
	 * 
	 * @throws IllegalArgumentException
	 *           if the record is too large to fit on one data page
	 */
	public RID insertRecord(byte[] record) {
		if (record.length > MAX_TUPSIZE) {
			throw new IllegalArgumentException("Record is too large");
		}
		PageId pageno = getAvailPage(record.length);
		DataPage page = new DataPage();
		Minibase.BufferManager.pinPage(pageno, page, PIN_MEMCPY);
		RID rid = page.insertRecord(record);
		updateDirEntry(pageno, 1, page.getFreeSpace());
		Minibase.BufferManager.unpinPage(pageno, UNPIN_DIRTY);
		return rid;
	} // public RID insertRecord(byte[] record)

	/**
	 * Reads a record from the file, given its rid.
	 * 
	 * @throws IllegalArgumentException
	 *           if the rid is invalid
	 */
	public byte[] selectRecord(RID rid) {
		DataPage page = new DataPage();
		Minibase.BufferManager.pinPage(rid.pageno, page, PIN_DISKIO);
		byte ret[] = page.selectRecord(rid);
		Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_CLEAN);
		return ret;
	} // public byte[] selectRecord(RID rid)

	/**
	 * Updates the specified record in the heap file.
	 * 
	 * @throws IllegalArgumentException
	 *           if the rid or new record is invalid
	 */
	public void updateRecord(RID rid, byte[] newRecord) {
		DataPage page = new DataPage();
		Minibase.BufferManager.pinPage(rid.pageno, page, PIN_NOOP);
		page.updateRecord(rid, newRecord);
		Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
	} // public void updateRecord(RID rid, byte[] newRecord)

	/**
	 * Deletes the specified record from the heap file. Removes empty data and/or
	 * directory pages.
	 * 
	 * @throws IllegalArgumentException
	 *           if the rid is invalid
	 */
	public void deleteRecord(RID rid) {
		DataPage page = new DataPage();
		Minibase.BufferManager.pinPage(rid.pageno, page, PIN_NOOP);
		page.deleteRecord(rid);
		Minibase.BufferManager.unpinPage(rid.pageno, UNPIN_DIRTY);
		updateDirEntry(rid.pageno, -1, page.getFreeSpace());
	} // public void deleteRecord(RID rid)

	/**
	 * Gets the number of records in the file.
	 */
	public int getRecCnt() {
		PageId pageno = new PageId(headId.pid);
		DirPage dirPage = new DirPage();
		int count = 0;

		// For all pages
		while (pageno.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_NOOP);
			for (int i = 0; i < dirPage.getEntryCnt(); i++) {
				count += dirPage.getRecCnt(i);
			}

			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno.copyPageId(dirPage.getNextPage());
		}

		return count;
	} // public int getRecCnt()

	/**
	 * Initiates a sequential scan of the heap file.
	 */
	public HeapScan openScan() {
		return new HeapScan(this);
	}

	/**
	 * Returns the name of the heap file.
	 */
	public String toString() {
		return fileName;
	}

	/**
	 * Searches the directory for the first data page with enough free space to
	 * store a record of the given size. If no suitable page is found, this creates
	 * a new data page. A more efficient implementation would start with a directory
	 * page that is in the buffer pool.
	 */
	protected PageId getAvailPage(int reclen) {
		PageId freePid = new PageId();
		PageId pageno = new PageId(headId.pid);
		DirPage dirPage = new DirPage();

		// For all pages
		while (pageno.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_NOOP);
			for (int i = 0; i < dirPage.getEntryCnt(); i++) {
				if (dirPage.getFreeCnt(i) < reclen + DirPage.IX_REC_CNT) {
					continue;
				}
				freePid = dirPage.getPageId(i);
				break;
			}

			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno.copyPageId(dirPage.getNextPage());
		}

		if (freePid.pid == INVALID_PAGEID) {
			freePid = insertPage();
		}

		return freePid;
	} // protected PageId getAvailPage(int reclen)

	/**
	 * Helper method for finding directory entries of data pages. A more efficient
	 * implementation would start with a directory page that is in the buffer pool.
	 * 
	 * @param pageno
	 *          identifies the page for which to find an entry
	 * @param dirId
	 *          output param to hold the directory page's id (pinned)
	 * @param dirPage
	 *          output param to hold directory page contents
	 * @return index of the data page's entry on the directory page
	 */
	protected int findDirEntry(PageId pageno, PageId dirId, DirPage dirPage) {
		dirId.pid = headId.pid;

		// For all pages
		while (dirId.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(dirId, dirPage, PIN_NOOP);
			for (int i = 0; i < dirPage.getEntryCnt(); i++) {
				if (pageno.pid == dirPage.getPageId(i).pid) {
					return i;
				}
			}

			Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
			dirId.copyPageId(dirPage.getNextPage());
		}
		return INVALID_PAGEID;
	} // protected int findEntry(PageId pageno, PageId dirId, DirPage dirPage)

	/**
	 * Updates the directory entry for the given data page. If the data page becomes
	 * empty, remove it. If this causes a dir page to become empty, remove it
	 * 
	 * @param pageno
	 *          identifies the data page whose directory entry will be updated
	 * @param deltaRec
	 *          input change in number of records on that data page
	 * @param freecnt
	 *          input new value of freecnt for the directory entry
	 */
	protected void updateDirEntry(PageId pageno, int deltaRec, int freecnt) {
		PageId dirId = new PageId();
		DirPage dirPage = new DirPage();

		int index = findDirEntry(pageno, dirId, dirPage); // Also pins the page
		int reccnt = dirPage.getRecCnt(index) + deltaRec;
		if (reccnt < 1) {
			deletePage(pageno, dirId, dirPage, index);
		} else {
			dirPage.setRecCnt(index, reccnt);
			dirPage.setFreeCnt(index, freecnt);
			Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
		}
	} // protected void updateEntry(PageId pageno, int deltaRec, int deltaFree)

	/**
	 * Inserts a new empty data page and its directory entry into the heap file. If
	 * necessary, this also inserts a new directory page. Leaves all data and
	 * directory pages unpinned
	 * 
	 * @return id of the new data page
	 */
	protected PageId insertPage() {
		PageId dirId = new PageId(headId.pid);
		DirPage dirPage = new DirPage();
		int index = 0;

		while (dirId.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(dirId, dirPage, PIN_NOOP);

			if (dirPage.getEntryCnt() < DirPage.MAX_ENTRIES) { // There is space
				index = dirPage.getEntryCnt();
				break;
			} else { // No Space, check the next directory
				PageId nextId = dirPage.getNextPage();

				if (nextId.pid == INVALID_PAGEID) { // If the next directory does not exist, make it
					DirPage newDirPage = new DirPage();
					PageId newDirId = Minibase.BufferManager.newPage(newDirPage, 1);
					newDirPage.setCurPage(newDirId);
					dirPage.setNextPage(newDirId);
					newDirPage.setPrevPage(dirId);
					Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
					dirId = newDirId;
					nextId = newDirId;
					dirPage = newDirPage;
					index = 0;
				}

				Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
				dirId = nextId;
			}
		}

		// Insert a new page & update the directory
		DataPage dataPage = new DataPage();
		PageId dataId = Minibase.BufferManager.newPage(dataPage, 1);
		dataPage.setCurPage(dataId);

		dirPage.setPageId(index, dataId);
		dirPage.setRecCnt(index, 0);
		dirPage.setFreeCnt(index, dataPage.getFreeSpace());
		dirPage.setEntryCnt(index + 1);

		Minibase.BufferManager.unpinPage(dataId, UNPIN_DIRTY);
		Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
		return dataId;
	} // protected PageId insertPage()

	/**
	 * Deletes the given data page and its directory entry from the heap file. If
	 * appropriate, this also deletes the directory page.
	 * 
	 * @param pageno
	 *          identifies the page to be deleted
	 * @param dirId
	 *          input param id of the directory page holding the data page's entry
	 * @param dirPage
	 *          input param to hold directory page contents
	 * @param index
	 *          input the data page's entry on the directory page
	 */
	protected void deletePage(PageId pageno, PageId dirId, DirPage dirPage, int index) {
		Minibase.BufferManager.freePage(pageno);
		dirPage.compact(index);

		int count = dirPage.getEntryCnt();
		if (count == 1 && dirId.pid != headId.pid) {
			DirPage page = new DirPage();
			PageId prevId = dirPage.getPrevPage();
			PageId nextId = dirPage.getNextPage();

			Minibase.BufferManager.pinPage(prevId, page, PIN_NOOP);
			page.setNextPage(nextId);
			Minibase.BufferManager.unpinPage(prevId, UNPIN_DIRTY);

			Minibase.BufferManager.unpinPage(dirId, UNPIN_CLEAN);
			Minibase.BufferManager.freePage(dirId);
		} else {
			dirPage.setEntryCnt(count - 1);
			Minibase.BufferManager.unpinPage(dirId, UNPIN_DIRTY);
		}
	} // protected void deletePage(PageId, PageId, DirPage, int)

} // public class HeapFile implements GlobalConst
