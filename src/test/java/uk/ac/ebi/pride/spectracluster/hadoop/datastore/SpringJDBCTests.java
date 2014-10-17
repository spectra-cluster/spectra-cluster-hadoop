package uk.ac.ebi.pride.spectracluster.hadoop.datastore;

import com.lordjoe.utilities.CollectionUtilities;
import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.pride.spectracluster.hadoop.ClusteringTestUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * uk.ac.ebi.pride.spectracluster.datastore.SpringJDBCTests
 * User: Steve
 * Date: 7/15/13
 */
public class SpringJDBCTests {


    public static final boolean SKIP_DATABASE_TESTS = true; // these tests are long and not critical


    /*
       NOTE - these  tests will work only when a MYSQL database is running on local_host and
        has a user pride-spectra password pride   has enough privileges to create and delete databases
     */

    /**
     * this test says you have a test database built and a proper table
     *
     * @throws Exception
     */
    @Test
    public void testDbAccessSetup() throws Exception {
        if (SKIP_DATABASE_TESTS)
            return;
        // run once with the right password - the machine will remember
        //   SpringJDBCUtilities.setHostPassword("localhost", "<secret>");
        DataSource ds = DataSourceDefaults.INSTANCE.getDefaultDataSource();
        final Connection connection = ds.getConnection();
        Assert.assertNotNull(connection);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("describe test.spectrums");
        while (resultSet.next()) {
            //noinspection UnusedDeclaration
            final String field = resultSet.getString("field");
            //noinspection UnusedDeclaration
            final String type = resultSet.getString("type");
        }
        connection.close();


    }

    /**
     * clear then load the database and make sure that
     * what is read back is equivalent to what is read
     *
     * @throws Exception
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Test
    public void testDbLoad() throws Exception {
        if (SKIP_DATABASE_TESTS)
            return;

        DataSource ds = DataSourceDefaults.INSTANCE.getDefaultDataSource();
        SpectrumDataStore db = new SpectrumDataStore("test", ds);

        db.clearAllData();


        Iterable<? extends ISpectrum> allSpectra = db.getAllSpectra();
        if (allSpectra.iterator().hasNext())
            return; // already loaded

        List<? extends ISpectrum> originalSpectra = ClusteringTestUtilities.readISpectraFromResource();

        db.storeSpectra(originalSpectra);

        //noinspection UnusedAssignment
        allSpectra = db.getAllSpectra();
        List<ISpectrum> holder = getAllSpectra(db);
        Collections.sort(holder);
        Collections.sort(originalSpectra);

        Assert.assertEquals(holder.size(), originalSpectra.size());

        for (int i = 0; i < holder.size(); i++) {
            ISpectrum orig = originalSpectra.get(i);
            if (orig.getPeaksCount() > SpectrumUtilities.MAXIMUM_ENCODED_PEAKS) {
                orig = new Spectrum(orig, SpectrumUtilities.filterTop250Peaks(orig.getPeaks()));
            }
            ISpectrum read = holder.get(i);
            Assert.assertTrue(orig.equivalent(read));
        }

    }


    public static final int NUMBER_MULTIPLE_LOAD_TEST = 10;

    /**
     * load the same spectra several times into the database and make sure that there are no errors
     * or duplicates
     *
     * @throws Exception
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Test
    public void testMultipleDbLoad() throws Exception {
        if (SKIP_DATABASE_TESTS)
            return;

        DataSource ds = DataSourceDefaults.INSTANCE.getDefaultDataSource();
        SpectrumDataStore db = new SpectrumDataStore("test", ds);

        // the returns list is a List<ISpectrum>
        List<ISpectrum> originalSpectra = (List<ISpectrum>) ((List) ClusteringTestUtilities.readISpectraFromResource());
        List<ISpectrum> holder = getAllSpectra(db);

        // load up as needed
        if (holder.size() != originalSpectra.size()) {
            db.storeSpectra(originalSpectra);
            holder = getAllSpectra(db);
        }


        // try duplicate inserts
        //noinspection UnusedDeclaration
        List<ISpectrum> tryToReinsert = CollectionUtilities.subList(holder, NUMBER_MULTIPLE_LOAD_TEST);
        db.storeSpectra(originalSpectra);

        List<ISpectrum> holder2 = getAllSpectra(db);

        // assert nothing more added (and no exception)
        Assert.assertEquals(holder.size(), holder2.size());


    }

    private List<ISpectrum> getAllSpectra(final SpectrumDataStore pDb) {
        //noinspection UnusedAssignment
        Iterable<? extends ISpectrum> allSpectra = pDb.getAllSpectra();
        allSpectra = pDb.getAllSpectra();
        List<ISpectrum> holder = new ArrayList<ISpectrum>();

        for (ISpectrum sc : allSpectra) {
            holder.add(sc);
        }
        return holder;
    }


    @Test
    public void testDbAccess() throws Exception {
        if (SKIP_DATABASE_TESTS)
            return;
        DataSource ds = DataSourceDefaults.INSTANCE.getDefaultDataSource();

        SpectrumDataStore db = new SpectrumDataStore("test", ds);

        final Iterable<? extends ISpectrum> allSpectra = db.getAllSpectra();
        List originalSpectra = ClusteringTestUtilities.readISpectraFromResource();


        List<ISpectrum> holder = new ArrayList<ISpectrum>();

        for (ISpectrum sc : allSpectra) {
            holder.add(sc);
        }

        Collections.sort(holder);
        Collections.sort(originalSpectra);

        Assert.assertEquals(holder.size(), originalSpectra.size());

        for (int i = 0; i < holder.size(); i++) {
            ISpectrum orig = (ISpectrum) originalSpectra.get(i);
            if (orig.getPeaksCount() > SpectrumUtilities.MAXIMUM_ENCODED_PEAKS) {
                orig = new Spectrum(orig, SpectrumUtilities.filterTop250Peaks(orig.getPeaks()));
            }
            ISpectrum read = holder.get(i);
            final boolean equivalent = orig.equivalent(read);
            Assert.assertTrue(equivalent);
        }


    }
}
