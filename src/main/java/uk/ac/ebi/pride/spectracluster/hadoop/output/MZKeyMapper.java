package uk.ac.ebi.pride.spectracluster.hadoop.output;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.hadoop.keys.MZKey;
import uk.ac.ebi.pride.spectracluster.hadoop.util.IOUtilities;
import uk.ac.ebi.pride.spectracluster.util.function.Functions;
import uk.ac.ebi.pride.spectracluster.util.function.IFunction;
import uk.ac.ebi.pride.spectracluster.util.predicate.IPredicate;
import uk.ac.ebi.pride.spectracluster.util.predicate.Predicates;
import uk.ac.ebi.pride.spectracluster.util.predicate.cluster.ClusterSizePredicate;

import java.io.IOException;

/**
 * Mapper that simply read clusters and pass them on using their precursor m/z as the key
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZKeyMapper extends Mapper<Text, Text, Text, Text> {

    /**
     * Reuse output text objects to avoid create many short lived objects
     */
    private Text keyOutputText = new Text();
    private Text valueOutputText = new Text();

    /**
     * Filter used to remove unqualified spectra
     */
    private IFunction<ICluster, ICluster> filter;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // create predicate for minimum number of spectra
        String miniNumOfSpectra = context.getConfiguration().get("mini.number.spectra", "");
        IPredicate<ICluster> miniNumOfSpectraPredicate = Predicates.alwaysTrue();
        if (miniNumOfSpectra != null) {
            miniNumOfSpectraPredicate = new ClusterSizePredicate(Integer.parseInt(miniNumOfSpectra));
        }

        // create function to filter empty peaks from consensus spectrum
        //String removeEmptyPeaks = context.getConfiguration().get("remove.empty.peaks", "");
        IFunction<ICluster, ICluster> removeClusterEmptyPeaksFunction = Functions.nothing();
        // JG: this has been disabled as it does not work with GreedySpectralClusters

        // combine predicate and function together
        IFunction<ICluster, ICluster> clusterFilter = Functions.condition(removeClusterEmptyPeaksFunction, miniNumOfSpectraPredicate);
        setFilter(clusterFilter);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (key.toString().length() == 0 || value.toString().length() == 0)
            return;

        ICluster[] clusters = IOUtilities.parseClustersFromCGFString(value.toString());

        for (ICluster cluster : clusters) {
            // filter cluster
            ICluster filteredCluster = getFilter().apply(cluster);

            if (filteredCluster != null) {
                MZKey mzkey = new MZKey(cluster.getPrecursorMz());

                keyOutputText.set(mzkey.toString());
                String filteredClusterString = IOUtilities.convertClusterToCGFString(filteredCluster);
                valueOutputText.set(filteredClusterString);
                context.write(keyOutputText, valueOutputText);
            }
        }
    }

    public IFunction<ICluster, ICluster> getFilter() {
        return filter;
    }

    public void setFilter(IFunction<ICluster, ICluster> filter) {
        this.filter = filter;
    }
}
