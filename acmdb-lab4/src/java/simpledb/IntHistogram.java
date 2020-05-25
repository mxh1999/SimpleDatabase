package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int siz;
    private int[] bucket;
    private int ntups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.bucket = new int[buckets];
    	this.ntups = 0;
    	this.siz = (max-min)/buckets+1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int id = (v-min)/siz;
    	bucket[id]++;
    	ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int id;
        double ans =0;
        switch (op) {
            case EQUALS:
                if (v<min || v>max) return 0;
                id = (v-min)/siz;
                return (double)bucket[id]/ntups/siz;
            case NOT_EQUALS:
                if (v<min || v>max) return 1;
                id = (v-min)/siz;
                return 1.0-(double)bucket[id]/ntups/siz;
            case LESS_THAN:
                if (v<min) return 0;
                if (v>max) return 1;
                id = (v-min)/siz;
                for (int i=0;i<id;i++)
                    ans += (double)bucket[i]/ntups;
                return ans;
            case LESS_THAN_OR_EQ:
                if (v<min) return 0;
                if (v>max) return 1;
                id = (v-min)/siz;
                for (int i=0;i<=id;i++)
                    ans += (double)bucket[i]/ntups;
                return ans;
            case GREATER_THAN:
                if (v<min) return 1;
                if (v>max) return 0;
                id = (v-min)/siz;
                for (int i=id+1;i<buckets;i++)
                    ans += (double)bucket[i]/ntups;
                return ans;
            case GREATER_THAN_OR_EQ:
                if (v<min) return 1;
                if (v>max) return 0;
                id = (v-min)/siz;
                for (int i=id;i<buckets;i++)
                    ans += (double)bucket[i]/ntups;
                return ans;
            default:
                return -1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return bucket.toString();
    }
}
