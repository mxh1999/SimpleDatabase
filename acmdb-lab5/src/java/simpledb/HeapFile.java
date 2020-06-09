package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        try {
            RandomAccessFile f = new RandomAccessFile(this.f,"r");
            int offset = BufferPool.getPageSize() * pid.pageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            f.seek(offset);
            f.readFully(data,0,BufferPool.getPageSize());
            f.close();
            return new HeapPage((HeapPageId)pid,data);
        } catch (IOException e) {
            //throw new IllegalArgumentException();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        RandomAccessFile f = new RandomAccessFile(this.f,"rw");
        f.seek(pid.pageNumber()*BufferPool.getPageSize());
        f.write(page.getPageData());
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        try {
            RandomAccessFile f=new RandomAccessFile(this.f, "r");
            int ans =(int)Math.ceil((double)f.length()/BufferPool.getPageSize());
            f.close();
            return ans;
        } catch (Exception ignore) {}
        return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtypages = new ArrayList<>();
        int id = getId();
        for (int i=0,j=numPages();i<j;i++) {
            HeapPageId pid = new HeapPageId(id,i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                dirtypages.add(page);
                return dirtypages;
            }
        }
        HeapPageId pid = new HeapPageId(id,numPages());
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        dirtypages.add(page);
        writePage(page);
        return dirtypages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> dirtypages = new ArrayList<>();
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        dirtypages.add(page);
        return dirtypages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }
    class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private HeapPage page;
        private Iterator<Tuple> iter;
        private int cnt;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.cnt = -1;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            cnt = 0;
            page = null;
            iter = null;
        }

        private boolean nxt() throws TransactionAbortedException, DbException {
            while (iter==null || !iter.hasNext()) {
                if (cnt>=numPages()) return false;
                page = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),cnt),Permissions.READ_WRITE);
                if (page==null) return false;
                iter = page.iterator();
                cnt++;
            }
            return true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (cnt==-1) return false;
            return nxt();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException();
            nxt();
            return iter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (cnt==-1) throw new NoSuchElementException();
            cnt = 0;
            page = null;
            iter = null;
        }

        @Override
        public void close() {
            cnt = -1;
        }
    }
}

