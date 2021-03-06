package simpledb;

import sun.util.resources.cldr.so.CalendarData_so_ET;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private LockManager lockManager;
    private ConcurrentHashMap<PageId, Page> pages;
    private int numpages;
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        numpages=numPages;
        pages = new ConcurrentHashMap<PageId, Page>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        int waitcnt = 0;
        while (!lockManager.getlock(tid,pid,perm)) {
            try {
                wait(100);
            } catch (InterruptedException ignore) {}
            waitcnt ++;
            if (waitcnt>10) {
                throw new TransactionAbortedException();
            }
        }
        if (pages.containsKey(pid)) return pages.get(pid);
        else {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page newpage = dbFile.readPage(pid);
            if (pages.size()>=numpages) evictPage();
            pages.put(pid,newpage);
            return newpage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.eraseTP(new LockManager.TP(tid,pid,Permissions.READ_WRITE));
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.queryTP(tid,p)!=null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        ArrayList<LockManager.TP> tmp = (ArrayList<LockManager.TP>) lockManager.query_tid(tid).clone();
        if (!commit) {
            ArrayList<Page> dirtypages = new ArrayList<>();
            for (Page page:pages.values()) {
                TransactionId id = page.isDirty();
                LockManager.TP tp = lockManager.queryTP(tid,page.getId());
                if (tp!=null) {
                    if ((id!=null && id.equals(tid))||tp.permissions.equals(Permissions.READ_WRITE)) dirtypages.add(page);
                }
            }
            for (Page page:dirtypages) pages.remove(page.getId());
        }   else flushPages(tid);
        for (LockManager.TP tp:tmp) lockManager.eraseTP(tp);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> tmp = f.insertTuple(tid,t);
        for (Page page:tmp) {
            page.markDirty(true,tid);
            if (pages.containsKey(page.getId())) {
                pages.remove(page.getId());
                pages.put(page.getId(),page);
            }   else {
                if (pages.size()>=numpages) evictPage();
                pages.put(page.getId(),page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> tmp = f.deleteTuple(tid,t);
        for (Page page:tmp) {
            page.markDirty(true,tid);
            if (pages.containsKey(page.getId())) {
                pages.remove(page.getId());
                pages.put(page.getId(),page);
            }   else {
                if (pages.size()>=numpages) evictPage();
                pages.put(page.getId(),page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page:pages.values()) {
            flushPage(page.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pages.get(pid);
        if (page != null && page.isDirty()!=null) {
            DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page.markDirty(false,null);
            f.writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (Page page: pages.values()) {
            TransactionId id = page.isDirty();
            if (id != null && id.equals(tid)) {
                page.markDirty(false,tid);
                PageId pid = page.getId();
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages.get(pid));
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        for (Page page:pages.values()) {
            if (page.isDirty()==null) {
                discardPage(page.getId());
                return;
            }
        }
        throw new DbException("");
    }

}
