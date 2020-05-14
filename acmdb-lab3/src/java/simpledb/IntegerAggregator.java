package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> ans;
    private HashMap<Field, Integer> sum,num;//for AVG
    private TupleDesc td;
    private HashMap<Field, Tuple> fin;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;
        this.ans = new HashMap<>();
        this.fin = new HashMap<>();
        if (what.equals(Op.AVG)) {
            this.sum = new HashMap<>();
            this.num = new HashMap<>();
        }   else {
            this.sum = this.num = null;
        }
        if (gbfield == NO_GROUPING)
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = null;
        if (gbfield != NO_GROUPING) key = tup.getField(gbfield);
        int val = ((IntField)tup.getField(afield)).getValue();
        switch (what) {
            case COUNT:
                ans.merge(key, 1, Integer::sum);
                break;
            case SUM:
                ans.merge(key, val,Integer::sum);
                break;
            case AVG:
                sum.merge(key, val, Integer::sum);
                num.merge(key, 1, Integer::sum);
                ans.put(key,sum.get(key)/num.get(key));
                break;
            case MAX:
                ans.merge(key, val, Integer::max);
                break;
            case MIN:
                ans.merge(key, val, Integer::min);
                break;
        }
        Tuple tp = new Tuple(td);
        if (gbfield == NO_GROUPING) {
            tp.setField(0,new IntField(ans.get(key)));
        }   else {
            tp.setField(0,key);
            tp.setField(1,new IntField(ans.get(key)));
        }
        fin.put(key,tp);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        return new TupleIterator(td,fin.values());
    }
}
