package uk.ac.ebi.pride.spectracluster.hadoop;

import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.clustersmilarity.*;
import uk.ac.ebi.pride.spectracluster.io.*;
import uk.ac.ebi.pride.spectracluster.spectrum.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import java.io.*;
import java.util.*;

/**
 * Utility methods for SpectrumInCluster
 *
 * @author Rui Wang
 * @version $Id$
 */
public final class SpectrumInClusterUtilities {

    public static final String PLACE_SPECTRUM_IN_BEST_CLUSTER = "uk.ac.ebi.pride.spectracluster.cluster.SpectrumInCluster.PlaceSpectrumInBestCluster";
    public static final String BREAK_UP_CLUSTERS_LESS_THAN = "uk.ac.ebi.pride.spectracluster.cluster.SpectrumInCluster.BreakUpClustersLessThan";

    
    public static final String BEGIN_IONS = "BEGIN IONS";
    public static final String END_IONS = "END IONS";
    public static final String BEGIN_CLUSTER = "BEGIN CLUSTER";
    public static final String END_CLUSTER = "END CLUSTER";
    public static final String BEGIN_CLUSTERING = "=Cluster=";

//
//    public static final String AVERAGE_PRECURSOR_MZ = "av_precursor_mz=";
//    public static final String AVERAGE_PRECURSOR_INTENSITY = "av_precursor_intens=";
//    public static final String PEPTIDE_SEQUENCE = "sequence=";
//    public static final String CONSENSUS_MZ = "consensus_mz=";
//    public static final String CONSENSUS_INTENSITY = "consensus_intens=";
//    public static final String SPECTRUM_ID = "SPEC";
    
    private SpectrumInClusterUtilities() {
    }

    public static SpectrumInCluster readSpectrumInCluster(String str) {
        LineNumberReader rdr = new LineNumberReader(new StringReader(str));
        return readSpectrumInCluster(rdr);
    }

    /**
     * Read clustering file into a set of clusters
     *
     * @param inp
     * @return
     */
    public static ICluster[] readClustersFromClusteringFile(LineNumberReader inp) {
        List<ICluster> holder = new ArrayList<ICluster>();

        try {
            String line = inp.readLine();
            while (line != null && !line.startsWith(BEGIN_CLUSTERING)) {
                line = inp.readLine();
            }


            List<String> clusterContent = new ArrayList<String>();
            while (line != null) {
                if (line.startsWith(BEGIN_CLUSTERING)) {
                    if (!clusterContent.isEmpty()) {
                        ICluster cluster = processIntoCluster(clusterContent,null );
                        if (cluster != null) {
                            holder.add(cluster);
                        }
                    }
                    clusterContent.clear();
                }
                else {
                    clusterContent.add(line);
                }

                line = inp.readLine();
            }

            if (!clusterContent.isEmpty()) {
                ICluster cluster = processIntoCluster(clusterContent,null);
                if (cluster != null) {
                    holder.add(cluster);
                }
            }
        }
        catch (IOException ioe) {
            throw new IllegalStateException("Failed to read ", ioe);
        }

        ICluster[] ret = new ICluster[holder.size()];
        holder.toArray(ret);
        return ret;
    }

    protected static ICluster processIntoCluster(List<String> clusterLines,ISpectrumRetriever spectrumRetriever) {
          LazyLoadedSpectralCluster cluster = new LazyLoadedSpectralCluster();

        String consensusMzLine = null;
        String consensusIntensityLine = null;
        for (String clusterLine : clusterLines) {
            if(clusterLine.length() == 0)
                break;
            if (clusterLine.startsWith("name=")) {
                break; // start of a new file
            }
            if (clusterLine.startsWith(ClusterParserUtilities.AVERAGE_PRECURSOR_MZ)) {
                float precursorMz = Float.parseFloat(clusterLine.replace(ClusterParserUtilities.AVERAGE_PRECURSOR_MZ, ""));
                cluster.setPrecursorMz(precursorMz);
            }
            else if (clusterLine.startsWith(ClusterParserUtilities.CONSENSUS_MZ)) {
                consensusMzLine = clusterLine.replace(ClusterParserUtilities.CONSENSUS_MZ, "");
            }
            else if (clusterLine.startsWith(ClusterParserUtilities.CONSENSUS_INTENSITY)) {
                consensusIntensityLine = clusterLine.replace(ClusterParserUtilities.CONSENSUS_INTENSITY, "");
            }
            else if (clusterLine.startsWith(ClusterParserUtilities.PEPTIDE_SEQUENCE)) {
                String peptideSequence = clusterLine.replace(ClusterParserUtilities.PEPTIDE_SEQUENCE, "");
                peptideSequence = peptideSequence.replace("[", "").replace("]", "");
                cluster.addPeptides(peptideSequence);
            }
            else if (clusterLine.startsWith(ClusterParserUtilities.SPECTRUM_ID)) {
                String[] parts = clusterLine.split("\t");
                String id = parts[1];
           //     ISpectrum spectrum = PSMSpectrum.getSpectrum(id );
                LazyLoadedSpectrum spectrum = new LazyLoadedSpectrum(parts[1], spectrumRetriever);
                 cluster.addSpectra(spectrum);
            }
            else //noinspection StatementWithEmptyBody
                if (clusterLine.startsWith(ClusterParserUtilities.AVERAGE_PRECURSOR_INTENSITY)) {
                    // do nothing here
                }
                else {
                    if (clusterLine.length() > 0) {
                        throw new IllegalArgumentException("cannot process line " + clusterLine);
                    }
                }
        }

        if (consensusIntensityLine == null)
            return null;

        List<IPeak> peaks = buildPeaks(consensusMzLine, consensusIntensityLine);
        if (peaks == null)
            return null;
        ISpectrum consensusSpectrum = new Spectrum(null,  cluster.getPrecursorCharge(),  cluster.getPrecursorMz(),Defaults.getDefaultQualityScorer(), peaks);
        cluster.setConsensusSpectrum(consensusSpectrum);

        return cluster;
    }

    public static List<IPeak> buildPeaks(String commaDelimitecMZ, String commaDelimitedIntensity) {
        try {
            float[] mzValues = parseCommaDelimitedFloats(commaDelimitecMZ);
            float[] intensityValues = parseCommaDelimitedFloats(commaDelimitedIntensity);
            if (mzValues.length != intensityValues.length)
                throw new IllegalArgumentException("Unequal mz and intensity lists");
            List<IPeak> holder = new ArrayList<IPeak>();
            for (int i = 0; i < intensityValues.length; i++) {
                holder.add(new Peak(mzValues[i], intensityValues[i]));
            }
            Collections.sort(holder);  // sort peaks by mz
            return holder;
        }
        catch (RuntimeException e) {
            return null;
        }
    }

    protected static float[] parseCommaDelimitedFloats(String commaDelimitedFloats) {
        String[] items = commaDelimitedFloats.trim().split(",");
        float[] ret = new float[items.length];
        for (int i = 0; i < items.length; i++) {
            String item = items[i];
            ret[i] = Float.parseFloat(item);
        }
        return ret;
    }


    /**
     * take a line like BEGIN CLUSTER Charge=2 Id=VVXVXVVX  return id
     *
     * @param line
     * @return
     */
    protected static String idFromClusterLine(String line) {
        line = line.replace(BEGIN_CLUSTER, "").trim();
        String[] split = line.split(" ");
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            if (s.startsWith("Id=")) {
                return s.substring("Id=".length());
            }
        }
        throw new IllegalArgumentException("no Id= part in " + line);
    }

    /**
     * take a line like BEGIN CLUSTER Charge=2 Id=VVXVXVVX  return charge
     *
     * @param line
     * @return
     */
    protected static int chargeFromClusterLine(String line) {
        line = line.replace(BEGIN_CLUSTER, "").trim();
        String[] split = line.split(" ");
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            if (s.startsWith("Charge=")) {
                return (int) (0.5 + Double.parseDouble(s.substring("Charge=".length())));
            }
        }
        throw new IllegalArgumentException("no Charge= part in " + line);
    }

    public static final String[] NOT_HANDLED_MGF_TAGS = {
            "TOLU=",
            "TOL=",
            "SEQ=",
            "COMP=",
            "TAG=",
            "ETAG=",
            "SCANS=",
            "IT_MODS=",
            "INSTRUMENT=",
    };

    /**
     * @param inp !null reader
     * @return
     */
    public static ISpectrum[] readMGFScans(LineNumberReader inp) {
        List<ISpectrum> holder = new ArrayList<ISpectrum>();
        ISpectrum spectrum = ParserUtilities.readMGFScan(inp);
        while (spectrum != null) {
            holder.add(spectrum);
            spectrum = ParserUtilities.readMGFScan(inp);
        }
        ISpectrum[] ret = new ISpectrum[holder.size()];
        holder.toArray(ret);
        return ret;
    }

    /**
     * @param inp !null existing file
     * @return !null array of spectra
     */
    public static ISpectrum[] readMGFScans(File inp) {
        try {
            return readMGFScans(new LineNumberReader(new FileReader(inp)));
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * read an mgf files and return as a list of single spectrum clusters
     *
     * @param inp !null existing file
     * @return !null array of spectra
     */
    public static List<ICluster> readMGFClusters(File inp) {
        ISpectrum[] scans = readMGFScans(inp);
        List<ICluster> holder = new ArrayList<ICluster>();
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < scans.length; i++) {
            ISpectrum scan = scans[i];
            final ICluster e = ClusterUtilities.asCluster(scan);
            holder.add(e);
        }

        return holder;
    }
    

    public static SpectrumInCluster readSpectrumInCluster(LineNumberReader rdr) {
        //todo: make this work

        try {
            String line = rdr.readLine();
            SpectrumInCluster ret = new SpectrumInCluster();
            while (line != null) {
                if ("=SpectrumInCluster=".equals(line.trim())) {
                    line = rdr.readLine();
                    break;
                }
            }

            if (!line.startsWith("removeFromCluster="))
                throw new IllegalStateException("badSpectrumInCluster");
            ret.setRemoveFromCluster(Boolean.parseBoolean(line.substring("removeFromCluster=".length())));
            line = rdr.readLine();

            if (!line.startsWith("distance="))
                throw new IllegalStateException("badSpectrumInCluster");
            double distance = Double.parseDouble(line.substring("distance=".length()));
            if (distance >= 0)    // todo fix later
                ret.setDistance(distance);

            ISpectrum spec = ParserUtilities.readMGFScan(rdr, line);

            ret.setSpectrum(spec);

            ICluster[] clusters =  readClustersFromClusteringFile(rdr);
            if (clusters.length != 1)
                throw new IllegalStateException("badSpectrumInCluster");

            ret.setCluster(clusters[0]);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }


    }
}
