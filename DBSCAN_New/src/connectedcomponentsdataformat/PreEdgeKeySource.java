package connectedcomponentsdataformat;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PreEdgeKeySource implements WritableComparable<PreEdgeKeySource> {

    public int source, destination;

    public PreEdgeKeySource() {
    }

    public PreEdgeKeySource(int source, int destination) {
        this.source = source;
        this.destination = destination;
    }

    public void readFields(DataInput in) throws IOException {
        source = in.readInt();
        destination = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(source);
        out.writeInt(destination);
    }

    @Override
    public String toString() {
        return (String.valueOf(source) + "," + String.valueOf(destination));
    }

    @Override
    public boolean equals(Object other) {
        PreEdgeKeySource obj = (PreEdgeKeySource) other;
        if (source == obj.source && destination == obj.destination) {
            return true;
        }
        return false;
    }

    

    /*
     * This method sorts based on the two vertices except that it moves the
     * vertic with negative destination (non-core object) to the end of the sorted
     * list. This is done to avoid keeping such points in reducer'e memory.
     */
    @Override
    public int compareTo(PreEdgeKeySource t) {

        if (source > t.source) {
            return 1;
        } else if (source < t.source) {
            return -1;
        } else {
            if (destination > t.destination) {
                return 1;
            } else if (destination < t.destination) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
