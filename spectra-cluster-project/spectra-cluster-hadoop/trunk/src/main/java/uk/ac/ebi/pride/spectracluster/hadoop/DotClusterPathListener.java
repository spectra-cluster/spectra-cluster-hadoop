package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import uk.ac.ebi.pride.spectracluster.cluster.*;
import uk.ac.ebi.pride.spectracluster.io.DotClusterClusterAppender;
import uk.ac.ebi.pride.spectracluster.io.IClusterAppender;
import uk.ac.ebi.pride.spectracluster.keys.*;
import uk.ac.ebi.pride.spectracluster.util.*;

import javax.annotation.*;
import java.io.*;

/**
 * uk.ac.ebi.pride.spectracluster.io.DotClusterFileListener
 * User: Steve
 * Date: 9/23/13
 */
public class DotClusterPathListener implements ClusterCreateListener {


    private final PrintWriter m_OutWriter;
    private final Path m_TempFile;
    private final Reducer.Context m_Context;
    private final Path m_PermFile;
    private final IClusterAppender m_Appender;
    private boolean anythingWritten;

    /**
     * creat with an output file
     *
     */
    public DotClusterPathListener(Path parent, MZKey key, @Nonnull final Reducer.Context context, IClusterAppender appender, PathFromMZGenerator generator) {
        m_Appender = appender;
        m_Context = context;
        try {
            m_TempFile = generator.generateTemporaryPath(parent, key, context);
            m_PermFile = generator.generatePath(parent, key, context);
            FileSystem fs = parent.getFileSystem(context.getConfiguration());
            final FSDataOutputStream dsOut = fs.create(m_TempFile);
            System.err.println("Making attempt path " + m_TempFile);
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            m_OutWriter = new PrintWriter(new OutputStreamWriter(dsOut));
            // add a header
            new DotClusterClusterAppender().appendDotClusterHeader(m_OutWriter, m_PermFile.getName());

        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public boolean isAnythingWritten() {
        return anythingWritten;
    }

    /**
     * can only go true
     *
     * @param pAnythingWritten
     */
    public void setAnythingWritten(final boolean pAnythingWritten) {
        anythingWritten |= pAnythingWritten;
    }

    /**
     * initialize reading - if reading happens once - sayt from
     * one file all this may happen in the constructor
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    @Override
    public void onClusterStarted(Object... otherData) {
        m_Appender.appendStart(m_OutWriter, m_PermFile.getName());
    }


    /**
     * do something when a cluster is created or read
     *
     * @param cluster
     */
    @Override
    public void onClusterCreate(final ICluster cluster, Object... otherData) {
        m_Appender.appendCluster(m_OutWriter, cluster);
        setAnythingWritten(true);
    }

    /**
     * do something when a cluster when the last cluster is read -
     * this may be after a file read is finished
     */
    @Override
    public void onClusterCreateFinished(Object... otherData) {
        m_OutWriter.close();
        if (isAnythingWritten()) {
            try {
                FileSystem fs = m_TempFile.getFileSystem(m_Context.getConfiguration());
                fs.rename(m_TempFile, m_PermFile);
            }
            catch (IOException e) {
                throw new RuntimeException(e);

            }
        }
    }


}
