package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;
    private HashMap<Field, Integer> ans;
    private HashMap<Field, Tuple> fin;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.ans = new HashMap<>();
        this.fin = new HashMap<>();
        if (gbfield == NO_GROUPING)
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = null;
        if (gbfield != NO_GROUPING) key = tup.getField(gbfield);
        if (what == Op.COUNT) {
            ans.merge(key, 1, Integer::sum);
        }
        Tuple tp = new Tuple(td);
        if (gbfield == NO_GROUPING) {
            tp.setField(0,new IntField(ans.get(key)));
        }   else {
            tp.setField(0,key);
            if (what == Op.COUNT)
                tp.setField(1,new IntField(ans.get(key)));
        }
        fin.put(key,tp);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new TupleIterator(td,fin.values());
    }
}
