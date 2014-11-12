package uk.ac.ebi.pride.spectracluster.hadoop.util;

import java.io.*;

/**
 * org.systemsbiology.hadoop.IStreamOpener
 *  interface abstracts the conversion of a String to an input Stream
 * @author Steve Lewis
 * @date Mar 8, 2011
 */
public interface IStreamOpener
{

    /**
     * open a file from a string
     * @param fileName string representing the file
     * @param otherData any other required data
     * @return possibly null stream
     */
    public InputStream open(String fileName, Object... otherData);


}

