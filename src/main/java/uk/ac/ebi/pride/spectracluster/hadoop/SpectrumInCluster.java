package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.algorithms.Equivalent;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.similarity.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.cluster.SpectrumInCluster
 * Used in a Hadoop step where the key is the spectrum id and the value is the spectrum in cluster
 * return says whether to keep the spectrum or not
 * User: Steve
 * Date: 4/7/14
 */
public class SpectrumInCluster implements Equivalent<SpectrumInCluster> {

    /**
     * sort by size highest first
     */
    public static final Comparator<SpectrumInCluster> BY_SIZE = new Comparator<SpectrumInCluster>() {
        @Override
        public int compare(final SpectrumInCluster o1, final SpectrumInCluster o2) {
            ICluster cluster1 = o1.getCluster();
            ICluster cluster2 = o2.getCluster();
            int diff = cluster1.getClusteredSpectraCount() - cluster2.getClusteredSpectraCount();
            if (diff != 0)
                return diff > 0 ? -1 : 1;
            return cluster1.compareTo(cluster2);
        }
    };

    public static Map<String, List<String>> serializeSpectrumInCluster(final Map<String, List<SpectrumInCluster>> pByClusterId) {
        Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (String key : pByClusterId.keySet()) {
            List<String> serializes = new ArrayList<String>();
            for (SpectrumInCluster s1 : pByClusterId.get(key)) {
                StringBuilder sb = new StringBuilder();
                s1.append(sb);
                serializes.add(sb.toString());
            }
            ret.put(key, serializes);
        }
        return ret;
    }


    public static Map<String, List<SpectrumInCluster>> mapByClusterContentsString(final Map<String, List<SpectrumInCluster>> pById) {
        // populate a list  by clusterId
        Map<String, List<SpectrumInCluster>> byClusterId = new HashMap<String, List<SpectrumInCluster>>();
        for (String s : pById.keySet()) {
            List<SpectrumInCluster> original = pById.get(s);
            List<SpectrumInCluster> copy = new ArrayList<SpectrumInCluster>(original);
            SpectrumInCluster.handleClusters(copy);
            for (SpectrumInCluster spectrumInCluster : copy) {
                String clusterIds = SpectrumInCluster.listClusterIds(spectrumInCluster.getCluster());
                if (!byClusterId.containsKey(clusterIds)) {
                    byClusterId.put(clusterIds, new ArrayList<SpectrumInCluster>());
                }
                byClusterId.get(clusterIds).add(spectrumInCluster);
            }

        }
        return byClusterId;
    }


    public static List<SpectrumInCluster> buildSpectrumInClusters(final List<ICluster> pScs) {
        List<SpectrumInCluster> inClusters = new ArrayList<SpectrumInCluster>();
        // turn into
        for (ICluster sc : pScs) {
            List<SpectrumInCluster> spectrumInClusters = SpectrumInCluster.fromCluster(sc);
            inClusters.addAll(spectrumInClusters);
        }
        return inClusters;
    }


    public static Map<String, List<SpectrumInCluster>> mapById(final List<SpectrumInCluster> pInClusters) {
        Map<String, List<SpectrumInCluster>> byId = new HashMap<String, List<SpectrumInCluster>>();
        for (SpectrumInCluster inCluster : pInClusters) {
            String id = inCluster.getCluster().getId();
            if (!byId.containsKey(id)) {
                byId.put(id, new ArrayList<SpectrumInCluster>());
            }
            byId.get(id).add(inCluster);
        }
        return byId;
    }

    public static String listClusterIds(ICluster sc) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        List<String> holder = new ArrayList<String>();

        for (ISpectrum iSpectrum : sc.getClusteredSpectra()) {
            holder.add(iSpectrum.getId());
        }
        String[] ret = new String[holder.size()];
        holder.toArray(ret);

        sb.append(ret[0]);
        for (int i = 1; i < ret.length; i++) {
            sb.append(",");
            sb.append(ret[i]);

        }
        sb.append("]");
        return sb.toString();
    }

    public static List<SpectrumInCluster> fromCluster(ICluster cluster) {
        List<SpectrumInCluster> holder = new ArrayList<SpectrumInCluster>();
        for (ISpectrum sc : cluster.getClusteredSpectra()) {
            if (sc instanceof ISpectrum) {
                holder.add(new SpectrumInCluster((ISpectrum) sc, cluster));
            }
        }
        return holder;

    }

    public static void handleClusters(List<SpectrumInCluster> clusters) {
        dropSingleClusters(clusters);
        if (clusters.size() == 1)  // only single clusters  or just one
        {
            return;
        }


        SpectrumInCluster best = findBestCluster(clusters);
        for (SpectrumInCluster cluster : clusters) {
            if (best == cluster)
                continue;
            cluster.setRemoveFromCluster(true);  // not best
        }
    }

    /**
     * if we fins a cluster contained in a larger cluster drop it
     *
     * @param pClusters
     */
    public static List<SpectrumInCluster> dropContainedClusters(final List<SpectrumInCluster> pClusters) {
        Set<SpectrumInCluster> toRemove = new HashSet<SpectrumInCluster>();
        List<SpectrumInCluster> bySize = new ArrayList<SpectrumInCluster>(pClusters);
        Collections.sort(bySize, SpectrumInCluster.BY_SIZE);
        for (int i = 0; i < bySize.size(); i++) {
            SpectrumInCluster test = pClusters.get(i);
            if (toRemove.contains(test))
                continue;
            final ICluster cluster1 = test.getCluster();
            Set<String> testIds = cluster1.getSpectralIds();
            for (int j = i + 1; j < bySize.size(); j++) {
                SpectrumInCluster test2 = pClusters.get(i);
                Set<String> testIds2 = test2.getCluster().getSpectralIds();
                if (testIds.containsAll(testIds2))
                    toRemove.add(test2);
            }

        }
        if (!toRemove.isEmpty()) {
            List<SpectrumInCluster> ret = new ArrayList<SpectrumInCluster>(pClusters);
            ret.removeAll(toRemove);
            return ret;
        } else {
            return pClusters;
        }
    }

    public static SpectrumInCluster findBestCluster(final List<SpectrumInCluster> pClusters) {
        List<SpectrumInCluster> bySize = new ArrayList<SpectrumInCluster>(pClusters);
        Collections.sort(bySize, SpectrumInCluster.BY_SIZE);
        SpectrumInCluster best = bySize.get(0);


        SpectrumInCluster nextbest = pClusters.get(1);
        // choose the largest
        if (best.getSize() > (int) (nextbest.getSize() * 3) / 2)
            return best;

        double bestDistance = best.getDistance();
        for (int i = 1; i < bySize.size(); i++) {
            SpectrumInCluster test = pClusters.get(i);
            double testDistance = test.getDistance();
            if (testDistance < bestDistance) {
                best = test;
                bestDistance = testDistance;
            }

        }
        return best;
    }


    public static void dropSingleClusters(List<SpectrumInCluster> clusters) {
        if (clusters.size() == 1)
            return;
        // find all clusters of size 1
        List<SpectrumInCluster> holder = new ArrayList<SpectrumInCluster>();
        for (SpectrumInCluster cluster : clusters) {
            if (cluster.getCluster().getClusteredSpectraCount() == 1) {
                holder.add(cluster);
            }
        }
        // drop then
        clusters.removeAll(holder);
        // if nothing left add one back
        if (clusters.size() == 0)
            clusters.add(holder.get(0));
    }

    private ISpectrum spectrum;
    private ICluster cluster;
    private double distance = -1;
    private boolean removeFromCluster;
    private boolean leadSpectrum; // is or was first spectrum in the cluster


    public SpectrumInCluster() {
    }

    public SpectrumInCluster(final ISpectrum pSpectrum, final ICluster pCluster) {
        spectrum = pSpectrum;
        cluster = pCluster;
    }

    public int getSize() {
        return getCluster().getClusteredSpectraCount();
    }

    public void append(Appendable out) {
        try {
            out.append("=SpectrumInCluster=\n");
            out.append("removeFromCluster=" + isRemoveFromCluster() + "\n");
            out.append("distance=" + getDistance() + "\n");
            out.append("NumPeaks: " + getSpectrum().getPeaks().size() + "\n");
            final MGFSpectrumAppender spectrumAppender = MGFSpectrumAppender.INSTANCE;
            spectrumAppender.appendSpectrum(out, getSpectrum());
            final DotClusterClusterAppender clusterAppender = new DotClusterClusterAppender();
            clusterAppender.appendCluster(out, getCluster());

        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    @Override
    public boolean equivalent(final SpectrumInCluster o) {
        if (isRemoveFromCluster() != o.isRemoveFromCluster())
            return false;
        if (isLeadSpectrum() != o.isLeadSpectrum())
            return false;
        double distance1 = getDistance();
        double distance2 = o.getDistance();
        if (Math.abs(distance1 - distance2) > 0.001)
            return false;
        if (!getSpectrum().equivalent(o.getSpectrum()))
            return false;
        ICluster cluster1 = getCluster();
        ICluster cluster2 = o.getCluster();
        if (!cluster1.equivalent(cluster2))
            return false;
        return true;
    }


    public ISpectrum getSpectrum() {
        return spectrum;
    }

    public void setSpectrum(final ISpectrum pSpectrum) {
        spectrum = pSpectrum;
    }

    public ICluster getCluster() {
        return cluster;
    }

    public void setCluster(final ICluster pCluster) {
        if (cluster == pCluster)
            return; // nothing to do
        cluster = pCluster;
        if (distance < 0)
            distance = computeDistance();
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(final double pDistance) {
        distance = pDistance;
    }

    protected double computeDistance() {
        ISimilarityChecker similarityChecker = Defaults.getDefaultSimilarityChecker();
        ISpectrum spectrum1 = getSpectrum();
        ICluster cluster1 = getCluster();
        ISpectrum spectrum2 = cluster1.getConsensusSpectrum();
        double distance = similarityChecker.assessSimilarity(spectrum1, spectrum2);
        return distance;
    }

    public boolean isRemoveFromCluster() {
        return removeFromCluster;
    }

    public void setRemoveFromCluster(final boolean pRemoveFromCluster) {
        removeFromCluster = pRemoveFromCluster;
    }

    public boolean isLeadSpectrum() {
        return leadSpectrum;
    }

    public void setLeadSpectrum(final boolean pLeadSpectrum) {
        leadSpectrum = pLeadSpectrum;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getSpectrum().getId());
        sb.append(":");
        sb.append(getCluster().getClusteredSpectraCount());
        sb.append("=>");
        sb.append(getCluster().getSpectralId());
        return sb.toString();
    }

}
