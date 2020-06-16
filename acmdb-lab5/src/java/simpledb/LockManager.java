package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

enum State{
    EMPTY,READ1,WRITE,READ2
}

public class LockManager {
    public static class TP{
        public TransactionId tid;
        public PageId pid;
        public Permissions permissions;
        public TP(TransactionId tid, PageId pid,Permissions permissions) {
            this.tid = tid;
            this.pid = pid;
            this.permissions = permissions;
        }
        public boolean equals(Object other) {
            if (!(other instanceof TP)) return false;
            return tid.equals(((TP) other).tid) && pid.equals(((TP) other).pid) && permissions.equals(((TP) other).permissions);
        }
    }
    private ConcurrentHashMap<TransactionId, ArrayList<TP> > TransLockPage;
    private ConcurrentHashMap<PageId, ArrayList<TP> > PageLock;
    public LockManager() {
        TransLockPage = new ConcurrentHashMap<>();
        PageLock = new ConcurrentHashMap<>();
    }
    public synchronized ArrayList<TP> query_tid(TransactionId tid) {
        if (!TransLockPage.containsKey(tid)) {
            TransLockPage.put(tid,new ArrayList<TP>());
        }
        return TransLockPage.get(tid);
    }
    public synchronized ArrayList<TP> query_pid(PageId pid) {
        if (!PageLock.containsKey(pid)) {
            PageLock.put(pid,new ArrayList<TP>());
        }
        return PageLock.get(pid);
    }
    public synchronized TP queryTP(TransactionId tid,PageId pid) {
        ArrayList<TP> ans = query_tid(tid);
        for (TP i:ans) {
            if (i.pid.equals(pid)) return i;
        }
        return null;
    }
    public synchronized void addTP(TP tp) {
        eraseTP(new TP(tp.tid,tp.pid,Permissions.READ_ONLY));
        eraseTP(new TP(tp.tid,tp.pid,Permissions.READ_WRITE));
        query_tid(tp.tid).add(tp);
        query_pid(tp.pid).add(tp);
    }
    public synchronized void eraseTP(TP tp) {
        query_tid(tp.tid).remove(tp);
        query_pid(tp.pid).remove(tp);
    }
    private synchronized State getState(PageId pid) {
        ArrayList<TP> tmp = query_pid(pid);
        if (tmp.size()<1) return State.EMPTY;
        if (tmp.size()>1) return State.READ2;
        if (tmp.get(0).permissions.equals(Permissions.READ_ONLY))   return State.READ1;
        return State.WRITE;
    }
    public synchronized boolean getlock(TransactionId tid,PageId pid, Permissions permissions) {
        State state = getState(pid);
        ArrayList<TP> tmp = query_pid(pid);
        TP tp = new TP(tid,pid,permissions);
        switch (state) {
            case EMPTY:
                addTP(tp);
                return true;
            case READ1:
                if (tmp.get(0).tid.equals(tid)) {
                    addTP(tp);
                    return true;
                }
                if (permissions.equals(Permissions.READ_WRITE)) return false;
                addTP(tp);
                return true;
            case READ2:
                if (permissions.equals(Permissions.READ_WRITE)) return false;
                addTP(tp);
                return true;
            case WRITE:
                return tmp.get(0).tid.equals(tid);
        }
        return false;
    }
}