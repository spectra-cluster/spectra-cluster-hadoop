package uk.ac.ebi.pride.spectracluster.keys;

/**
 * uk.ac.ebi.pride.spectracluster.keys.IPartitionable
 * User: Steve
 * implemented by an object with knowledge about its own partition
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
