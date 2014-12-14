package uk.ac.ebi.pride.spectracluster.hadoop.util;

import uk.ac.ebi.pride.spectracluster.cluster.ICluster;
import uk.ac.ebi.pride.spectracluster.io.CGFClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.DotClusterClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.MGFSpectrumAppender;
import uk.ac.ebi.pride.spectracluster.io.ParserUtilities;
import uk.ac.ebi.pride.spectracluster.spectrum.ISpectrum;

import java.io.LineNumberReader;
import java.io.StringReader;

/**
 * @author Rui Wang
 * @version $Id$
 */
public final class IOUtilities {

    /**
     * Convert spectrum to string
     *
     * @param spectrum given spectrum
     * @return string represents spectrum
     */
    public static String convertSpectrumToMGFString(ISpectrum spectrum) {
        StringBuilder sb = new StringBuilder();
        MGFSpectrumAppender.INSTANCE.appendSpectrum(sb, spectrum);
        return sb.toString();
    }

    /**
     * Parse a given string into a spectrum
     *
     * @param spectrumString given string content
     * @return parsed spectrum
     */
    public static ISpectrum parseSpectrumFromMGFString(String spectrumString) {
        LineNumberReader reader = new LineNumberReader(new StringReader(spectrumString));
        return ParserUtilities.readMGFScan(reader);
    }

    /**
     * convert a cluster to string for output
     *
     * @param cluster given cluster
     * @return string   represents cluster in CGF format
     */
    public static String convertClusterToCGFString(ICluster cluster) {
        StringBuilder sb = new StringBuilder();
        CGFClusterAppender clusterAppender = CGFClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        return sb.toString();
    }


    /**
     * Parse a given string into a cluster
     *
     * @param clusterString cluster string in CGF format
     * @return a cluster object
     */
    public static ICluster parseClusterFromCGFString(String clusterString) {
        LineNumberReader rdr = new LineNumberReader((new StringReader(clusterString)));
        return ParserUtilities.readSpectralCluster(rdr, null);
    }

    /**
     * Parse a group of clusters
     * @param clusterString string that represents a group of cluster
     * @return  an array of clusters
     */
    public static ICluster[] parseClustersFromCGFString(String clusterString) {
        LineNumberReader rdr = new LineNumberReader((new StringReader(clusterString)));
        return ParserUtilities.readSpectralCluster(rdr);
    }


    /**
     * convert a cluster to string for output
     *
     * @param cluster given cluster
     * @return string   represents cluster in CGF format
     */
    public static String convertClusterToClusteringString(ICluster cluster) {
        StringBuilder sb = new StringBuilder();
        DotClusterClusterAppender clusterAppender = DotClusterClusterAppender.INSTANCE;
        clusterAppender.appendCluster(sb, cluster);
        return sb.toString();
    }

}
