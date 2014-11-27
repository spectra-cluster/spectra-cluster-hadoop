package uk.ac.ebi.pride.spectracluster.hadoop.keys;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class BinMZKeyTest {

    private BinMZKey binMZKey;

    @Before
    public void setUp() throws Exception {
        binMZKey = new BinMZKey(11, 120.50);
    }

    @Test
    public void testGetBin() throws Exception {
        assertEquals(11, binMZKey.getBin());
    }

    @Test
    public void testGetPrecursorMZ() throws Exception {
        assertEquals(120.50, binMZKey.getPrecursorMZ(), 0.00001);
    }

    @Test
    public void testToString() throws Exception {
        assertEquals("000011:0000120500", binMZKey.toString());
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(binMZKey, new BinMZKey(11, 120.50));
    }

    @Test
    public void testHashCode() throws Exception {
        assertEquals(-800812544, binMZKey.hashCode());
    }


    @Test
    public void testGetPartitionHash() throws Exception {
        assertEquals(1420005920, binMZKey.getPartitionHash());
    }
}