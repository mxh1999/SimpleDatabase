package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private TupleDesc td;
    private boolean called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
        called = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) return null;
        called = true;
        int cnt = 0;
        while (child.hasNext()) {
            cnt++;
            Tuple tmp = child.next();
            try{
                Database.getBufferPool().insertTuple(tid,tableid,tmp);
            } catch (IOException ignore) {};
        }
        Tuple t = new Tuple(td);
        t.setField(0,new IntField(cnt));
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] ans = new DbIterator[1];
        ans[0]=child;
        return ans;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }
}
