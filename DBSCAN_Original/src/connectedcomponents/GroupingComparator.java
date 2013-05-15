/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package connectedcomponents;

import connectedcomponentsdataformat.EdgeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(EdgeKey.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        EdgeKey key01 = (EdgeKey) o1;
        EdgeKey key02 = (EdgeKey) o2;

        return compareSource(key01.source, key02.source);
    }

    private int compareSource(int src1, int src2) {
        if (src1 > src2) {
            return 1;
        } else if (src1 < src2) {
            return -1;
        } else {
            return 0;
        }
    }
}
