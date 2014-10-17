package uk.ac.ebi.pride.spectracluster.hadoop;

import com.lordjoe.hadoop.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import uk.ac.ebi.pride.spectracluster.keys.*;

import javax.annotation.*;
import java.io.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.PathFromMZGenerator
 * User: Steve
 * Date: 9/25/13
 */
public class PathFromMZGenerator implements PathFromObjectGenerator<MZKey> {
    public static final PathFromMZGenerator CGF_INSTANCE = new PathFromMZGenerator("ClusterBin", "cgf");
    public static final PathFromMZGenerator BIG_CGF_INSTANCE = new PathFromMZGenerator("BigClusterBin", "cgf");
    public static final PathFromMZGenerator CLUSTERING_INSTANCE = new PathFromMZGenerator("ClusteringBin", "clustering");
    public static final PathFromMZGenerator BIG_CLUSTERING_INSTANCE = new PathFromMZGenerator("BigClusteringBin", "clustering");
    public static final PathFromMZGenerator SPTEXT_CLUSTERING_INSTANCE = new PathFromMZGenerator("SPLib", "sptxt");
    public static final PathFromMZGenerator MSP_CLUSTERING_INSTANCE = new PathFromMZGenerator("MSPLib", "msp");

    private final String extension;
    private final String baseName;

    private PathFromMZGenerator(final String pBaseName, final String pExtension) {
        extension = pExtension;
        baseName = pBaseName;
    }

     @SuppressWarnings("UnusedDeclaration")
    public String getExtension() {
        return extension;
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getBaseName() {
        return baseName;
    }

    /**
     * create a temporary path including the tast attempt
     *
     * @param parent    - parent path
     * @param target    target object
     * @param context   context to get tast attempt
     * @param otherData any other data often nothing
     * @return generated path
     */
    @Nonnull
    @Override
    public Path generateTemporaryPath(@Nonnull final Path parent, @Nonnull final MZKey key, @Nonnull final Reducer.Context context, final Object... otherData) {

        try {
            FileSystem fs = parent.getFileSystem(context.getConfiguration());
            String realBase = baseName + String.format("%04d", key.getAsInt());
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            Path path = SpectraHadoopUtilities.getAttempPath(context, fs, parent, realBase);
            return path;
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    /**
     * create a premanent path including the tast attempt
     *
     * @param parent    - parent path
     * @param target    target object
     * @param context   context to get tast attempt
     * @param otherData any other data often nothing
     * @return generated path
     */
    @Nonnull
    @Override
    public Path generatePath(@Nonnull final Path parent, @Nonnull final MZKey key, @Nonnull final Reducer.Context context, final Object... otherData) {
        try {
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            FileSystem fs = parent.getFileSystem(context.getConfiguration());
            String realBase = baseName + String.format("%04d", key.getAsInt());
            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            Path path = new Path(parent, realBase + "." + extension);
            return path;
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    /**
     * rename the temporary path to the permenent one
     *
     * @param parent    - parent path
     * @param target    target object
     * @param context   context to get tast attempt
     * @param otherData any other data often nothing
     * @return generated path
     */
    @Nonnull
    @Override
    public Path renameTemporaryPath(@Nonnull final Path parent, @Nonnull final MZKey target, @Nonnull final Reducer.Context context, final Object... otherData) {

        try {
            Path temp = generateTemporaryPath(parent, target, context, otherData);
            Path perm = generatePath(parent, target, context, otherData);
            FileSystem fs = parent.getFileSystem(context.getConfiguration());
            fs.rename(temp, perm);
            return perm;
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }
}
