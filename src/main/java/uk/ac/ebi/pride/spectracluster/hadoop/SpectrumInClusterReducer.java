package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.io.Text;
import org.systemsbiology.hadoop.AbstractParameterizedReducer;
import org.systemsbiology.hadoop.ISetableParameterHolder;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.SpectrumInClusterReducer
 * <p/>
 * combine clusters for a single spectrum
 */
@Deprecated
public class SpectrumInClusterReducer extends AbstractParameterizedReducer {

    private boolean spectrumInBestCluster;

    @Override protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);

        ISetableParameterHolder application = getApplication();
        spectrumInBestCluster = application.getBooleanParameter(SpectrumInClusterUtilities.PLACE_SPECTRUM_IN_BEST_CLUSTER,false);
        throw new UnsupportedOperationException("This Does not work");
    }


    @Override
    public void reduceNormal(Text key, Iterable<Text> values,
                             Context context) throws IOException, InterruptedException {

        String spectrumId = key.toString();
        int numberProcessed = 0;

        // grab all spectraInClusters - these are guaranteed to be small
        List<SpectrumInCluster> passedClusters = SpectrumInClusterReducer.parseSpectrumInClusterFromValues(values);

        if (passedClusters.size() == 0)
            return;

        // drop smaller clusters contained in larger
        // Note - everywhere they are seem these will be dropped
        List<SpectrumInCluster> handler = SpectrumInCluster.dropContainedClusters(passedClusters);

        SpectrumInCluster.handleClusters(handler);
        for (SpectrumInCluster spectrumInCluster : handler) {
            writeSpectrumInCluster(spectrumInCluster, context);
        }

    }

    public static List<SpectrumInCluster> parseSpectrumInClusterFromValues(final Iterable<Text> values) {
        // grab all spectraInClusters - these are guaranteed to be small
        List<SpectrumInCluster> passedClusters = new ArrayList<SpectrumInCluster>();
        //noinspection LoopStatementThatDoesntLoop
        for (Text val : values) {
            String valStr = val.toString();
            LineNumberReader rdr = new LineNumberReader((new StringReader(valStr)));
            SpectrumInCluster sci2 = SpectrumInClusterUtilities.readSpectrumInCluster(rdr);
            passedClusters.add(sci2);
        }
        return passedClusters;
    }


    protected void writeSpectrumInCluster(SpectrumInCluster inCluster, Context context) {
        String id = inCluster.getCluster().getSpectralId();
        StringBuilder sb = new StringBuilder();
        inCluster.append(sb);
        writeKeyValue(id, sb.toString(), context);
    }

}
