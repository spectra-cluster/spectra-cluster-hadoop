package uk.ac.ebi.pride.spectracluster.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Output clusters into .clustering file format
 *
 * @author Rui Wang
 * @version $Id$
 */
public class ClusteringFileOutputFormat extends FileOutputFormat<NullWritable, Text> {

    private static final String CLUSTERING_FILE_EXTENSION = ".clustering";

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        Configuration configuration = context.getConfiguration();
        String prefix = configuration.get("clustering.file.prefix", "");

        FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getUniqueFile(context, prefix, extension));
    }


    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration configuration = job.getConfiguration();

        Path outputFile = this.getDefaultWorkFile(job, CLUSTERING_FILE_EXTENSION);
        FileSystem fs = outputFile.getFileSystem(configuration);
        FSDataOutputStream fileOut = fs.create(outputFile, false);
        return new ClusteringFileRecordWriter(fileOut);
    }


    private static class ClusteringFileRecordWriter extends RecordWriter<NullWritable, Text> {

        private final DataOutputStream out;

        public ClusteringFileRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(NullWritable key, Text value) throws IOException, InterruptedException {
            out.write(value.getBytes(), 0, value.getLength());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            out.close();
        }
    }

}
