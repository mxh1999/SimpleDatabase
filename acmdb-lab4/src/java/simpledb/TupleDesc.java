package simpledb;

import sun.nio.cs.ext.TIS_620;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private TDItem[] data_list;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new Iterator<TDItem>() {
            private int pos;
            @Override
            public boolean hasNext() {
                return pos < data_list.length;
            }

            @Override
            public TDItem next() {
                return data_list[pos++];
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        data_list = new TDItem[typeAr.length];
        for (int i=0;i<typeAr.length;i++) {
            data_list[i]=new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        data_list = new TDItem[typeAr.length];
        for (int i=0;i<typeAr.length;i++) {
            data_list[i]=new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return data_list.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i<0 || i>data_list.length)
            throw new NoSuchElementException();
        return data_list[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i<0 || i>data_list.length)
            throw new NoSuchElementException();
        return data_list[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) throw new NoSuchElementException();
        for (int i = 0;i<data_list.length;i++) {
            if (name.equals(getFieldName(i)))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sum = 0;
        for (TDItem tdItem : data_list) {
            sum += tdItem.fieldType.getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int len = td1.numFields() + td2.numFields();
        Type[] ans_type = new Type[len];
        String[] ans_name = new String[len];
        for (int i = 0;i<td1.numFields();i++){
            ans_type[i] = td1.getFieldType(i);
            ans_name[i] = td1.getFieldName(i);
        }
        for (int i = 0;i<td2.numFields();i++){
            ans_type[i+td1.numFields()] = td2.getFieldType(i);
            ans_name[i+td1.numFields()] = td2.getFieldName(i);
        }
        return new TupleDesc(ans_type,ans_name);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o instanceof TupleDesc) {
            if (getSize()!=((TupleDesc) o).getSize() || numFields()!=((TupleDesc) o).numFields()) return false;
            for (int i=0;i<numFields();i++) {
                if (!getFieldType(i).equals(((TupleDesc) o).getFieldType(i))) return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer ans = new StringBuffer();
        for (int i = 0 ;i<data_list.length;i++) {
            ans.append(data_list[i].fieldType).append("[").append(i).append("](").append(data_list[i].fieldName).append("),");
        }
        return ans.toString();
    }
}
