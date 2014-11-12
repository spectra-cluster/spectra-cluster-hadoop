package review.uk.ac.ebi.pride.spectracluster.hadoop.keys;

/**
 * Implemented by an object with knowledge about its own partition
 * User: Steve
 * Date: 9/2/2014
 */
public interface IPartitionable {


    /**
     * here is an int that a partitioner would use
     *
     * @return
     */
    public int getPartitionHash();

}
