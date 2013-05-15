/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ccpreprocessing2;
import connectedcomponentsdataformat.PreEdgeKeySource;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class PreSortingComparatorSource extends WritableComparator {

    protected PreSortingComparatorSource() {
        super(PreEdgeKeySource.class, true);
    }

    // compare based on both source and destination vertices
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        PreEdgeKeySource key01 = (PreEdgeKeySource) o1;
        PreEdgeKeySource key02 = (PreEdgeKeySource) o2;
        

        return key01.compareTo(key02);
    }
}