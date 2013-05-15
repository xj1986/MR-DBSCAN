/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package connectedcomponents;

import connectedcomponentsdataformat.EdgeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SortingComparator extends WritableComparator {

    protected SortingComparator() {
        super(EdgeKey.class, true);
    }

    // compare based on both source and destination vertices
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        EdgeKey key01 = (EdgeKey) o1;
        EdgeKey key02 = (EdgeKey) o2;

        return key01.compareTo(key02);
    }
}
