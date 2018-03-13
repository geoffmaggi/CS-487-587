package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;
//import heap.DirPage;

/**
 * <h3>Minibase Hash Index</h3> This unclustered index implements static hashing
 * as described on pages 371 to 373 of the textbook (3rd edition). The index
 * file is a stored as a heapfile.
 */
public class HashIndex implements GlobalConst {

	/** File name of the hash index. */
	protected String fileName;

	/** Page id of the directory. */
	protected PageId headId;

	// Log2 of the number of buckets - fixed for this simple index
	protected final int DEPTH = 7;

	// --------------------------------------------------------------------------

	/**
	 * Opens an index file given its name, or creates a new index file if the name
	 * doesn't exist; a null name produces a temporary index file which requires no
	 * file library entry and whose pages are freed when there are no more
	 * references to it. The file's directory contains the locations of the 128
	 * primary bucket pages. You will need to decide on a structure for the
	 * directory. The library entry contains the name of the index file and the
	 * pageId of the file's directory.
	 */
	public HashIndex(String fileName) {
		if (fileName != null) {
			this.fileName = fileName;
			headId = Minibase.DiskManager.get_file_entry(fileName);

			if (headId == null) { // new page
				HashDirPage hPage = new HashDirPage();
				headId = Minibase.BufferManager.newPage(hPage, 1);
				Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
				Minibase.DiskManager.add_file_entry(fileName, headId);
			}
		} else { // Temp Page
			HashDirPage hPage = new HashDirPage();
			headId = Minibase.BufferManager.newPage(hPage, 1);
			Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
		}
	} // public HashIndex(String fileName)

	/**
	 * Called by the garbage collector when there are no more references to the
	 * object; deletes the index file if it's temporary.
	 */
	protected void finalize() throws Throwable {
		if (fileName == null) { // Temp Page
			deleteFile();
		}
	} // protected void finalize() throws Throwable

	/**
	 * Deletes the index file from the database, freeing all of its pages.
	 */
	public void deleteFile() {
		PageId pageno = new PageId(headId.pid);
		HashDirPage dirPage = new HashDirPage();

		// For all pages
		while (pageno.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);

			for (int i = 0; i < dirPage.getEntryCount(); i++) {
				PageId pid = dirPage.getNextPage();
				HashBucketPage hPage = new HashBucketPage();

				while (pid.pid != INVALID_PAGEID) { // For all pages
					Minibase.BufferManager.pinPage(pid, hPage, PIN_DISKIO);
					PageId nextId = hPage.getNextPage();
					Minibase.BufferManager.unpinPage(pid, UNPIN_CLEAN);
					Minibase.BufferManager.freePage(pid);
					pid = nextId;
				}
			}

			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			Minibase.BufferManager.freePage(pageno);
			pageno.copyPageId(dirPage.getNextPage());
		}

		if (fileName != null) { // Not a temp page
			Minibase.DiskManager.delete_file_entry(fileName);
		}
	} // public void deleteFile()

	/**
	 * Inserts a new data entry into the index file.
	 * 
	 * @throws IllegalArgumentException
	 *           if the entry is too large
	 */
	public void insertEntry(SearchKey key, RID rid) {
		DataEntry entry = new DataEntry(key, rid);

		if (entry.getLength() > MAX_TUPSIZE) {
			throw new IllegalArgumentException("Record is too large");
		}

		int hash = key.getHash(DEPTH);
		PageId pageno = new PageId(headId.pid);
		HashDirPage dirPage = new HashDirPage();

		while (hash >= HashDirPage.MAX_ENTRIES) { // Find a page with space
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);
			PageId nextId = dirPage.getNextPage();
			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno = nextId;
			hash -= HashDirPage.MAX_ENTRIES;
		}

		Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);

		PageId pid = dirPage.getPageId(hash);
		HashBucketPage dataPage = new HashBucketPage();

		if (pid.pid != INVALID_PAGEID) { // existing page
			Minibase.BufferManager.pinPage(pid, dataPage, PIN_DISKIO);
			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
		} else { // new page
			pid = Minibase.BufferManager.newPage(dataPage, 1);
			dirPage.setPageId(hash, pid);
			Minibase.BufferManager.unpinPage(pageno, UNPIN_DIRTY);
		}

		dataPage.insertEntry(entry);
		Minibase.BufferManager.unpinPage(pid, UNPIN_DIRTY);
	} // public void insertEntry(SearchKey key, RID rid)

	/**
	 * Deletes the specified data entry from the index file.
	 * 
	 * @throws IllegalArgumentException
	 *           if the entry doesn't exist
	 */
	public void deleteEntry(SearchKey key, RID rid) {
		DataEntry entry = new DataEntry(key, rid);

		int hash = key.getHash(DEPTH);
		PageId pageno = new PageId(headId.pid);
		HashDirPage dirPage = new HashDirPage();

		while (hash >= HashDirPage.MAX_ENTRIES) { // find the right page
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);
			PageId nextId = dirPage.getNextPage();
			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno = nextId;
			hash -= HashDirPage.MAX_ENTRIES;
		}

		Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);

		PageId newPid = dirPage.getPageId(hash);
		Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
		HashBucketPage dataPage = new HashBucketPage();

		if (newPid.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(newPid, dataPage, PIN_DISKIO);
		} else {
			throw new IllegalArgumentException("entry doesn't exist");
		}

		try {
			dataPage.deleteEntry(entry);
			Minibase.BufferManager.unpinPage(newPid, UNPIN_DIRTY);
		} catch (IllegalArgumentException exc) {
			Minibase.BufferManager.unpinPage(newPid, UNPIN_CLEAN);
			throw exc;
		}
	} // public void deleteEntry(SearchKey key, RID rid)

	/**
	 * Initiates an equality scan of the index file.
	 */
	public HashScan openScan(SearchKey key) {
		return new HashScan(this, key);
	}

	/**
	 * Returns the name of the index file.
	 */
	public String toString() {
		return fileName;
	}

	/**
	 * Prints a high-level view of the directory, namely which buckets are allocated
	 * and how many entries are stored in each one. Sample output:
	 * 
	 * <pre>
	 * IX_Customers
	 * ------------
	 * 0000000 : 35
	 * 0000001 : null
	 * 0000010 : 27
	 * ...
	 * 1111111 : 42
	 * ------------
	 * Total : 1500
	 * </pre>
	 */
	public void printSummary() {
		String fileName = this.fileName == null ? "temp" : this.fileName;
		System.out.println();
		System.out.println(fileName);

		System.out.print("-------------");

		System.out.println();

		int total = 0;

		PageId pageno = new PageId(headId.pid);
		HashDirPage dirPage = new HashDirPage();
		HashBucketPage dataPage = new HashBucketPage();

		while (pageno.pid != INVALID_PAGEID) {
			Minibase.BufferManager.pinPage(pageno, dirPage, PIN_DISKIO);
			int count = dirPage.getEntryCount();

			for (int i = 0; i < count; i++) {
				String hash = Integer.toString(i, 2);
				for (int j = 0; j < DEPTH - hash.length(); j++) {
					System.out.print('0');
				}
				System.out.print(hash + " : ");

				PageId pid = dirPage.getPageId(i);
				if (pid.pid != INVALID_PAGEID) {
					Minibase.BufferManager.pinPage(pid, dataPage, PIN_DISKIO);
					int pageCount = dataPage.countEntries();
					System.out.println(pageCount);
					total += pageCount;
					Minibase.BufferManager.unpinPage(pid, UNPIN_CLEAN);
				} else {
					System.out.println("null");
				}
			}

			PageId nextId = dirPage.getNextPage();
			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno = nextId;

		}

		System.out.print("-------------");
		System.out.println();
		System.out.println("Total : " + total);
	} // public void printSummary()

} // public class HashIndex implements GlobalConst
