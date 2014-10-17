package uk.ac.ebi.pride.spectracluster.hadoop;

import org.junit.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.cluster.SpectrumInClusterTests
 * User: Steve
 * Date: 4/7/14
 *
 */
public class SpectrumInClusterTests {

    public static final String TEST_DATA =
            "=SpectrumInCluster=\n" +
                    "removeFromCluster=false\n" +
                    "distance=0.76\n" +

                    "BEGIN IONS\n" +
                    "CHARGE=2\n" +
                    "PEPMASS=400.25\n" +
                    "TITLE=id=1247848,sequence=LLGGLAVR\n" +
                    "128.967\t12.610\n" +
                    "136.021\t36.830\n" +
                    "137.982\t39.130\n" +
                    "141.895\t33.080\n" +
                    "153.995\t33.330\n" +
                    "155.031\t28.910\n" +
                    "159.172\t41.200\n" +
                    "165.149\t14.550\n" +
                    "175.120\t218.300\n" +
                    "183.206\t78.050\n" +
                    "187.041\t48.740\n" +
                    "191.151\t30.730\n" +
                    "199.082\t2421.000\n" +
                    "200.045\t40.620\n" +
                    "203.490\t20.230\n" +
                    "214.953\t37.330\n" +
                    "223.623\t109.900\n" +
                    "227.081\t6571.000\n" +
                    "228.068\t75.640\n" +
                    "229.366\t21.990\n" +
                    "232.962\t90.230\n" +
                    "234.093\t41.060\n" +
                    "240.222\t46.610\n" +
                    "251.115\t435.900\n" +
                    "251.952\t40.310\n" +
                    "255.390\t69.010\n" +
                    "259.167\t115.000\n" +
                    "269.239\t45.160\n" +
                    "271.128\t97.160\n" +
                    "274.201\t728.000\n" +
                    "281.313\t41.940\n" +
                    "284.153\t488.700\n" +
                    "287.069\t132.400\n" +
                    "288.384\t155.400\n" +
                    "292.268\t56.570\n" +
                    "296.203\t100.800\n" +
                    "297.079\t105.400\n" +
                    "299.231\t78.250\n" +
                    "300.295\t18.030\n" +
                    "304.875\t148.500\n" +
                    "313.418\t49.250\n" +
                    "314.435\t16.890\n" +
                    "325.893\t29.640\n" +
                    "328.697\t30.230\n" +
                    "339.029\t58.320\n" +
                    "340.315\t54.670\n" +
                    "341.280\t251.200\n" +
                    "342.039\t31.590\n" +
                    "345.361\t2266.000\n" +
                    "355.199\t147.600\n" +
                    "356.751\t110.100\n" +
                    "360.271\t20.040\n" +
                    "364.545\t41.770\n" +
                    "365.190\t46.870\n" +
                    "369.402\t79.560\n" +
                    "370.505\t21.020\n" +
                    "372.072\t33.010\n" +
                    "377.865\t167.200\n" +
                    "379.831\t67.160\n" +
                    "380.674\t22.730\n" +
                    "381.564\t313.700\n" +
                    "382.482\t141.400\n" +
                    "383.320\t16.620\n" +
                    "385.157\t161.200\n" +
                    "386.321\t154.000\n" +
                    "387.763\t84.300\n" +
                    "390.860\t1982.000\n" +
                    "391.963\t50.110\n" +
                    "393.309\t161.000\n" +
                    "409.498\t33.200\n" +
                    "426.472\t332.900\n" +
                    "429.581\t107.000\n" +
                    "441.320\t29.990\n" +
                    "442.307\t56.550\n" +
                    "452.142\t21.700\n" +
                    "454.204\t1858.000\n" +
                    "458.489\t354.400\n" +
                    "462.538\t99.180\n" +
                    "466.363\t85.720\n" +
                    "476.091\t70.120\n" +
                    "483.052\t67.380\n" +
                    "488.589\t41.840\n" +
                    "497.472\t164.300\n" +
                    "507.372\t39.190\n" +
                    "510.305\t35.930\n" +
                    "511.265\t32.280\n" +
                    "512.712\t83.080\n" +
                    "514.853\t578.000\n" +
                    "515.604\t303.000\n" +
                    "522.169\t312.400\n" +
                    "525.263\t526.200\n" +
                    "527.344\t28.460\n" +
                    "532.975\t34.690\n" +
                    "537.811\t73.890\n" +
                    "538.517\t47.880\n" +
                    "542.518\t151.500\n" +
                    "543.738\t33.500\n" +
                    "546.559\t22.100\n" +
                    "548.516\t576.100\n" +
                    "554.365\t64.860\n" +
                    "570.724\t40.520\n" +
                    "572.480\t23750.000\n" +
                    "573.494\t225.200\n" +
                    "578.451\t41.110\n" +
                    "583.102\t39.200\n" +
                    "597.379\t77.860\n" +
                    "616.012\t56.870\n" +
                    "624.380\t321.000\n" +
                    "637.407\t52.960\n" +
                    "661.562\t275.800\n" +
                    "665.370\t43.070\n" +
                    "669.747\t87.370\n" +
                    "684.917\t124.500\n" +
                    "685.598\t1121.000\n" +
                    "715.665\t20.490\n" +
                    "728.840\t60.530\n" +
                    "END IONS\n" +
                    "=Cluster=\n" +
                    "av_precursor_mz=400.435\n" +
                    "av_precursor_intens=1.0\n" +
                    "sequence=[]\n" +
                    "consensus_mz=136.021,143.075,171.030,175.135,199.094,200.130,227.046,228.085,274.265,284.164,341.163,343.567,345.280,346.308,390.899,426.267,441.407,454.207,455.282,458.379,514.899,522.288,525.225,572.382,573.404,606.358,624.262,625.342,685.494,686.529,706.597,725.441,742.359,767.527,784.275,800.426,811.273,812.261,838.675,872.555,921.297,930.120,1103.598,1562.117,1606.378\n" +
                    "consensus_intens=5998.61426,16390.90820,98933.33594,61072.00781,649138.18750,53391.42188,1457623.50000,133987.45313,93950.18750,72245.67188,59690.95703,155439.57813,401478.81250,41874.37500,318247.53125,72049.42969,105453.20313,312929.96875,58473.76563,78506.53906,852820.43750,106910.98438,100832.30469,3973312.25000,963287.62500,3529.29858,92351.31250,15595.06836,115205.92969,27619.35547,289.34476,307.61911,579.75061,353.04437,630.55914,128.38637,366.44003,96.25556,96.28497,208.85866,264.85892,56.08521,10.14625,7.19200,154.08858\n" +
                    "SPEC\t1247848\ttrue\n" +
                    "SPEC\t120702\ttrue\n" +
                    "SPEC\t121475\ttrue\n" +
                    "SPEC\t121479\ttrue\n" +
                    "SPEC\t151420\ttrue\n" +
                    "SPEC\t151427\ttrue\n" +
                    "SPEC\t151644\ttrue\n" +
                    "SPEC\t151654\ttrue\n" +
                    "SPEC\t217272\ttrue\n" +
                    "SPEC\t217276\ttrue\n" +
                    "SPEC\t26639\ttrue\n" +
                    "SPEC\t26642\ttrue\n" +
                    "SPEC\t298899\ttrue\n" +
                    "SPEC\t298905\ttrue\n" +
                    "SPEC\t298907\ttrue\n" +
                    "SPEC\t417696\ttrue\n" +
                    "SPEC\t755002\ttrue\n" +
                    "SPEC\t8099\ttrue\n" +
                    "SPEC\t8102\ttrue\n" +
                    "SPEC\t8107\ttrue\n";

    @Test
    public void testSpectrumInClusterRead() {
        //todo: make this unit test work
        List<ISpectrum> spectrums = ClusteringTestUtilities.readConsensusSpectralItems();
//        for (ISpectrum spectrum : spectrums) {
//               StringBuilder sb = new StringBuilder();
//            spectrum.appendSPText(sb);
//            System.out.println(sb);
//        }

        LineNumberReader rdr = new LineNumberReader(new StringReader(TEST_DATA));
        SpectrumInCluster sci = SpectrumInClusterUtilities.readSpectrumInCluster(rdr);

        StringBuilder sb = new StringBuilder();
        sci.append(sb);
        System.out.println(sb);

        rdr = new LineNumberReader(new StringReader(sb.toString()));
        SpectrumInCluster sci2 = SpectrumInClusterUtilities.readSpectrumInCluster(rdr);

        boolean equivalent = sci.equivalent(sci2);
        Assert.assertTrue(equivalent);
    }

    @Test
    public void testSpectrumInClusterMap() {
        //todo: make this unit test work
        // get some clusters
        List<ICluster> scs = ClusteringTestUtilities.readSpectraClustersFromResource();
        // build spectral associations
        List<SpectrumInCluster> inClusters = SpectrumInCluster.buildSpectrumInClusters(scs);
        //  map bu spectrun id - now we know all clusters for a spectrum
        Map<String, List<SpectrumInCluster>> byId = SpectrumInCluster.mapById(inClusters);
        // map by cluster contents
        Map<String, List<SpectrumInCluster>> byClusterId = SpectrumInCluster.mapByClusterContentsString(byId);
        // map serialized data by cluster
        Map<String, List<String>> byClusterIdSerialized = SpectrumInCluster.serializeSpectrumInCluster(byClusterId);

        // can we rebuild the clusters
        List<ICluster> scs2 = rebuildClusters(byClusterId, false);  // rebuild all
        ClusteringTestUtilities.assertEquivalentClusters(scs, scs2);

        // can we serialize rebuild the clusters
        List<ICluster> scs3 = rebuildClustersFromSerialization(byClusterIdSerialized, false);  // rebuild all
        ClusteringTestUtilities.assertEquivalentClusters(scs, scs3);

    }

    public static List<ICluster> rebuildClustersFromSerialization(final Map<String, List<String>> pByClusterId, boolean discardNotOnCLuster) {
        List<String> keys = new ArrayList<String>(pByClusterId.keySet());
        Collections.sort(keys);
        List<ICluster> holder = new ArrayList<ICluster>();
        for (String key : keys) {
            holder.add(rebuildSerializedCluster(pByClusterId.get(key), discardNotOnCLuster));
        }


        return holder;
    }


    /**
     * rebuild a cluster from a list of SpectrumInCluster
     *
     * @param ccs
     * @return
     */
    public static ICluster rebuildSerializedCluster(final List<String> ccs, boolean discardNotOnCLuster) {
        SpectralCluster ret = new SpectralCluster((String)null, Defaults.getDefaultConsensusSpectrumBuilder());
        for (String ccStr : ccs) {
            SpectrumInCluster cc = SpectrumInClusterUtilities.readSpectrumInCluster(ccStr);
            if (!discardNotOnCLuster || !cc.isRemoveFromCluster())
                ret.addSpectra(cc.getSpectrum());
        }
        return ret;
    }


    public static List<ICluster> rebuildClusters(final Map<String, List<SpectrumInCluster>> pByClusterId, boolean discardNotOnCLuster) {
        List<String> keys = new ArrayList<String>(pByClusterId.keySet());
        Collections.sort(keys);
        List<ICluster> holder = new ArrayList<ICluster>();
        for (String key : keys) {
            holder.add(rebuildCluster(pByClusterId.get(key), discardNotOnCLuster));
        }


        return holder;
    }

    /**
     * rebuild a cluster from a list of SpectrumInCluster
     *
     * @param ccs
     * @return
     */
    public static ICluster rebuildCluster(final List<SpectrumInCluster> ccs, boolean discardNotOnCLuster) {
        ICluster ret = new SpectralCluster((String)null,Defaults.getDefaultConsensusSpectrumBuilder());
        for (SpectrumInCluster cc : ccs) {
            if (!discardNotOnCLuster || !cc.isRemoveFromCluster())
                ret.addSpectra(cc.getSpectrum());
        }
        return ret;
    }


}
