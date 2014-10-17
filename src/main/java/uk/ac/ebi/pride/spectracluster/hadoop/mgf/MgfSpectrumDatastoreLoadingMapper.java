package uk.ac.ebi.pride.spectracluster.hadoop.mgf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.systemsbiology.hadoop.AbstractParameterizedMapper;
import org.systemsbiology.hadoop.ISetableParameterHolder;
import uk.ac.ebi.pride.spectracluster.hadoop.datastore.DataSourceDefaults;
import uk.ac.ebi.pride.spectracluster.hadoop.datastore.SpectrumDataStore;
import uk.ac.ebi.pride.spectracluster.hadoop.hbase.HBaseUtilities;
import uk.ac.ebi.pride.spectracluster.hadoop.hbase.PhoenixWorkingClusterDatabase;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

/**
 * Hadoop mapper for loading a group of mgf files into HBase using Phoenix
 *
 * @author Rui Wang
 * @version $Id$
 */
public class MgfSpectrumDatastoreLoadingMapper extends AbstractParameterizedMapper<Writable> {

    private SpectrumDataStore spectrumDataStore;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);

        ISetableParameterHolder application = getApplication();

        String tableName = application.getParameter("table_name");

        DataSource source = HBaseUtilities.getHBaseDataSource();
        DataSourceDefaults.INSTANCE.setDefaultDataSource(source);
        DataSourceDefaults.INSTANCE.setDatabaseFactory(PhoenixWorkingClusterDatabase.FACTORY);

        this.spectrumDataStore = new SpectrumDataStore(tableName, source);
    }

    @Override
    public void map(Writable key, Text value, Context context) throws IOException, InterruptedException {
        String label = key.toString();
        String text = value.toString();

        if (label == null || text == null || label.length() == 0 || text.length() == 0)
            return;

        LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
        final ISpectrum match = ParserUtilities.readMGFScan(rdr);
        if(match == null)
            spectrumDataStore.storeSpectrum(match);
    }


}
