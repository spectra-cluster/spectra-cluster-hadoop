package uk.ac.ebi.pride.spectracluster.hadoop.io;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * This runner is for running MGFInputFormat on a given MGF file
 * <p/>
 * This can used for testing the correctness of MGF file
 *
 * @author Rui Wang
 * @version $Id$
 */
public class MGFInputFormatRunner {

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: [input MGF file] [big spectra output file]");
            System.exit(1);
        }

        // input mgf file
        File mgfFile = new File(args[0]);
        System.out.println("Parsing MGF file: " + mgfFile.getAbsolutePath());

        // configure the input format
        RecordReader<Text, Text> recordReader = getTextTextRecordReader(mgfFile);

        // create writer for output file
        PrintWriter writer = new PrintWriter(new FileWriter(args[1]));

        // count the number of spectra
        try {
            int count = 0;
            while (recordReader.nextKeyValue()) {
                count++;
                Text currentValue = recordReader.getCurrentValue();
                String str = currentValue.toString();
                if (str.length() > 7000) {
                    System.out.println(str.length());
                    writer.print(str);
                    writer.flush();
                }

            }

            System.out.println("Read " + count + " spectra");
        } catch (Exception e) {
            // catch out of memory exception
            e.printStackTrace();
        }

        recordReader.close();
        writer.close();
    }

    private static RecordReader<Text, Text> getTextTextRecordReader(File mgfFile) throws IOException, InterruptedException {
        JobConf configuration = new JobConf(false);
        configuration.set("fs.default.name", "file:///");
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());

        Path path = new Path(mgfFile.getAbsoluteFile().toURI().toString());
        FileSplit split = new FileSplit(path, 0, mgfFile.length(), null);

        MGFInputFormat mgfInputFormat = ReflectionUtils.newInstance(MGFInputFormat.class, configuration);
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID());

        RecordReader<Text, Text> recordReader = mgfInputFormat.createRecordReader(split, taskAttemptContext);

        recordReader.initialize(split, taskAttemptContext);
        return recordReader;
    }
}
