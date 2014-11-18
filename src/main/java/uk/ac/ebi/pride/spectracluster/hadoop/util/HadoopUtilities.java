package uk.ac.ebi.pride.spectracluster.hadoop.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for Hadoop related things
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class HadoopUtilities {

    /**
     * write the jobs counters to a file called fileName in fileSystem
     *
     * @param fileSystem !null
     * @param fileName   !null
     * @param job        !null
     */
    public static void saveCounters(FileSystem fileSystem, String fileName, Job job) {
        Map<String, Long> counters = getAllJobCounters(job);
        Path p = new Path(fileName);
        PrintWriter out = null;
        try {
            FSDataOutputStream os = fileSystem.create(p, true); // create with overwrite
            out = new PrintWriter(new OutputStreamWriter(os));
            Set<String> strings = counters.keySet();
            String[] items = strings.toArray(new String[strings.size()]);
            Arrays.sort(items);
            for (String s : items) {
                out.println(s + "=" + counters.get(s));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);

        } finally {
            if (out != null)
                out.close();
        }
    }

    /**
     * Get all the counters generated by a job
     *
     * @param job
     * @return
     */
    public static Map<String, Long> getAllJobCounters(Job job) {
        try {
            Map<String, Long> ret = new HashMap<String, Long>();
            Counters counters = job.getCounters();
            for (CounterGroup counterGroup : counters) {
                for (Counter c : counterGroup) {
                    String counterName = c.getName();
                    long value = c.getValue();

                    ret.put(counterName, value);
                }
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }

    }
}
