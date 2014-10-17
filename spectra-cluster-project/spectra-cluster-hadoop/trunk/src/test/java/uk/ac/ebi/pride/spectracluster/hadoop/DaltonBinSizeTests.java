package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.conf.*;
import org.junit.*;
import org.systemsbiology.hadoop.*;
import org.systemsbiology.remotecontrol.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.DaltonBinSizeTests
 *   pretty hard coded test to see if we can read counters and get a sample count for
 *   later runs - depends on a hard coded path
 * @author Steve Lewis
 * @date 31/10/13
 */
public class DaltonBinSizeTests {

    // hard coded results for sample data set  spectra_400.0_4.0
    public static final int[] TEST_BINS = { 400,401,402,403};
    public static final int[] TEST_COUNTS = { 58,57,29,66};


    // todo generalize
    public static final String DEFAULT_SAMPLE_PATH = "/user/slewis/clustering/Samples";


    @Test
    public void testBinsSize() throws Exception {
        if(true)
            return; // do not do for now
        Configuration conf = new Configuration();
        String host = RemoteUtilities.getHost();
        int port = RemoteUtilities.getPort();
        String  userDir =   "/";
        conf.set("fs.default.name", "hdfs://" + host + ":" + port + userDir);
        conf.set("fs.defaultFS", "hdfs://" + host + ":" + port + userDir);

        conf.set(DefaultParameterHolder.PATH_KEY,DEFAULT_SAMPLE_PATH);

        int total = 0;
        for (int i = 0; i < TEST_BINS.length; i++) {
            int testBin = TEST_BINS[i];
            int count = DaltonBinSize.getNumberSpectraWithMZ(testBin,conf);
            Assert.assertEquals(TEST_COUNTS[i],count);
            total += count;
        }

        Assert.assertEquals(DaltonBinSize.getTotalSpectra(conf),total);
      }
}
