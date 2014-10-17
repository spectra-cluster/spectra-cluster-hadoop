package uk.ac.ebi.pride.spectracluster.hadoop;


import org.apache.hadoop.io.*;
import org.systemsbiology.hadoop.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SpectrumInClusterMapper
 * write each spectrum as a SpectrumToCluster with the spectrum id as the key
 * This guarantees that all clusters containing a spectrum go to one place
 * see  uk.ac.ebi.pride.spectracluster.hadoop.SpectrumInClusterReducer
 *
 * User: Steve
 * Date: 8/14/13
 */
public class SpectrumInClusterMapper extends AbstractParameterizedMapper<Text> {




    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
       }



    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String label = key.toString();
        String text = value.toString();
        if (label == null || text == null)
            return;
        if (label.length() == 0 || text.length() == 0)
            return;


        LineNumberReader rdr = new LineNumberReader((new StringReader(text)));
        ICluster[] clusters = ParserUtilities.readSpectralCluster(rdr);

        switch (clusters.length) {
            case 0:
                return;
            case 1:
                handleCluster(clusters[0],  context);
                return;
            default:
                throw new IllegalStateException("We got " + clusters.length +
                        " clusters - expected only 1"); //
        }
    }

    /**
     * write each spectrum as a SpectrumToCluster with the spectrum id as the key
     * This guarantees that all clusters containing a spectrum go to one place
     * @param cluster
     * @param context
     */
    protected void handleCluster(ICluster cluster,   Context context) {
        List<ISpectrum> clusteredSpectra = cluster.getClusteredSpectra();
        for (ISpectrum sc : clusteredSpectra) {
            SpectrumInCluster spectrumInCluster = new SpectrumInCluster((ISpectrum) sc, cluster);
            String id = sc.getId();
            StringBuilder sb = new StringBuilder();
            spectrumInCluster.append(sb);
            writeKeyValue(id, sb.toString(), context);
        }
       }



}
