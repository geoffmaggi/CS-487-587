/* Minibase Implementation for CS 487/587
 * Docs: http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/minibase.html
 * Authors: Alexander Goddard & Geoff Maggi
 * Github: https://github.com/geoffmaggi/CS-487-587
 */

package index;

import global.Minibase;
import global.PageId;

/**
 * An object in this class is a page in a linked list. The entire linked list is
 * a hash table bucket.
 */
class HashBucketPage extends SortedPage {

	/**
	 * Gets the number of entries in this page and later (overflow) pages in the
	 * list. <br>
	 * <br>
	 * To find the number of entries in a bucket, apply countEntries to the primary
	 * page of the bucket.
	 */
	public int countEntries() {
		int count = getEntryCount();

		PageId pageno = getNextPage();
		HashBucketPage hPage = new HashBucketPage();

		while (pageno.pid != INVALID_PAGEID) { // For all pages
			Minibase.BufferManager.pinPage(pageno, hPage, PIN_DISKIO);
			count += hPage.getEntryCount();
			PageId nextId = hPage.getNextPage();
			Minibase.BufferManager.unpinPage(pageno, UNPIN_CLEAN);
			pageno = nextId;
		}

		return count;
	} // public int countEntries()

	/**
	 * Inserts a new data entry into this page. If there is no room on this page,
	 * recursively inserts in later pages of the list. If necessary, creates a new
	 * page at the end of the list. Does not worry about keeping order between
	 * entries in different pages. <br>
	 * <br>
	 * To insert a data entry into a bucket, apply insertEntry to the primary page
	 * of the bucket.
	 * 
	 * @return true if inserting made this page dirty, false otherwise
	 */
	public boolean insertEntry(DataEntry entry) {
		try {
			super.insertEntry(entry);
			return true;
		} catch (Exception e) {
			PageId pageno = getNextPage();
			HashBucketPage hPage = new HashBucketPage();

			if (pageno.pid != INVALID_PAGEID) { // For all pages recursive
				Minibase.BufferManager.pinPage(pageno, hPage, PIN_DISKIO);
				boolean inserted = hPage.insertEntry(entry);
				Minibase.BufferManager.unpinPage(pageno, inserted ? UNPIN_DIRTY : UNPIN_CLEAN);
				return inserted;
			} else { // we are the last page
				pageno = Minibase.BufferManager.newPage(hPage, 1);
				setNextPage(pageno);
				hPage.insertEntry(entry);
				Minibase.BufferManager.unpinPage(pageno, UNPIN_DIRTY);
				return true;
			}
		}
	} // public boolean insertEntry(DataEntry entry)

	/**
	 * Deletes a data entry from this page. If a page in the list (not the primary
	 * page) becomes empty, it is deleted from the list.
	 * 
	 * To delete a data entry from a bucket, apply deleteEntry to the primary page
	 * of the bucket.
	 * 
	 * @return true if deleting made this page dirty, false otherwise
	 * @throws IllegalArgumentException
	 *           if the entry is not in the list.
	 */
	public boolean deleteEntry(DataEntry entry) {
		try {
			super.deleteEntry(entry);
			return true;
		} catch (Exception e) {
			PageId pageno = getNextPage();
			HashBucketPage hPage = new HashBucketPage();

			if (pageno.pid != INVALID_PAGEID) { // For all pages recursive
				Minibase.BufferManager.pinPage(pageno, hPage, PIN_DISKIO);
				boolean deleted = hPage.deleteEntry(entry);

				if (hPage.getEntryCount() < 1) { // if empty remove it
					setNextPage(hPage.getNextPage());
					Minibase.BufferManager.unpinPage(pageno, deleted ? UNPIN_DIRTY : UNPIN_CLEAN);
					Minibase.BufferManager.freePage(pageno);
				} else {
					Minibase.BufferManager.unpinPage(pageno, deleted ? UNPIN_DIRTY : UNPIN_CLEAN);
				}
				return deleted;
			} else {
				throw new IllegalArgumentException("Unable to delete entry: Entry not found");
			}
		}
	} // public boolean deleteEntry(DataEntry entry)

} // class HashBucketPage extends SortedPage
