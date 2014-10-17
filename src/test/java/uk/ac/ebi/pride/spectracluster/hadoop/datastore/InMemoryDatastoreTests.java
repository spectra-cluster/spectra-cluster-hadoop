package uk.ac.ebi.pride.spectracluster.hadoop.datastore;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.hadoop.ClusteringTestUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.util.ArrayList;
import java.util.List;

/**
 * uk.ac.ebi.pride.spectracluster.datastore.DatastoreTests
 * User: Steve
 * Date: 7/15/13
 */
public class InMemoryDatastoreTests {

    private IMutableSpectrumDataStore ds;

    @Before
    public void setUp() throws Exception {

        ds = new InMemoryDatastore();
    }

    @Test
    public void testSpectrumStore() throws Exception {
        final List<ISpectrum> spectrums = loadDataStrore();

        // make sure we have them
        for (ISpectrum spectrum : spectrums) {
            ISpectrum test = ds.getSpectrumById(spectrum.getId());
            Assert.assertTrue(test.equivalent(spectrum));
        }
    }

    @Test
    public void testSpectrumRemove() throws Exception {
        final List<ISpectrum> spectrums = loadDataStrore();

        List<ISpectrum> holder = new ArrayList<ISpectrum>();


        // make sure we have them
        for (ISpectrum spectrum : ds.getAllSpectra()) {
            holder.add(spectrum);
        }
        Assert.assertEquals(holder.size(), spectrums.size());
        for (ISpectrum spectrum : holder) {
            ds.removeSpectrum(spectrum);
        }
        holder.clear();
        // should be none left
        for (ISpectrum spectrum : ds.getAllSpectra()) {
            holder.add(spectrum);
        }
        Assert.assertEquals(0, holder.size());

    }

    @Test
    public void testClear() throws Exception {
        final List<ISpectrum> spectrums = loadDataStrore();

        List<ISpectrum> holder = new ArrayList<ISpectrum>();


        // make sure we have them
        for (ISpectrum spectrum : ds.getAllSpectra()) {
            holder.add(spectrum);
        }
        Assert.assertEquals(holder.size(), spectrums.size());

        ds.clearAllData();
        holder.clear();
        // should be none left
        for (ISpectrum spectrum : ds.getAllSpectra()) {
            holder.add(spectrum);
        }
        Assert.assertEquals(0, holder.size());

    }

    protected List<ISpectrum> loadDataStrore() {
        final List<ISpectrum> spectrums = ClusteringTestUtilities.readISpectraFromResource();
        // add then all
        for (ISpectrum spectrum : spectrums) {
            ds.storeSpectrum(spectrum);
        }
        return spectrums;
    }
}
