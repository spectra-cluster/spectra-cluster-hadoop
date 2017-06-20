package uk.ac.ebi.pride.spectracluster.hadoop.util;

import uk.ac.ebi.pride.spectracluster.engine.EngineFactories;
import uk.ac.ebi.pride.spectracluster.engine.IClusteringEngine;
import uk.ac.ebi.pride.spectracluster.util.IDefaultingFactory;

import java.io.IOException;

/**
 *
 * AbstractClusterReducer using classic clustering engine instead of incremental clustering engine
 *
 * @author Rui Wang
 * @version $Id$
 */
public abstract class AbstractClusterReducer extends FilterSingleSpectrumClusterReducer {
    private final IDefaultingFactory<IClusteringEngine> engineFactory = EngineFactories.DEFAULT_CLUSTERING_ENGINE_FACTORY;
    private IClusteringEngine engine;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // read and customize configuration, default will be used if not provided
        ConfigurableProperties.configureAnalysisParameters(context.getConfiguration());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        updateEngine(context, null);
        super.cleanup(context);
    }

    protected abstract <T> void updateEngine(final Context context, final T key) throws IOException, InterruptedException;

    public IClusteringEngine getEngine() {
        return engine;
    }

    public IDefaultingFactory<IClusteringEngine> getEngineFactory() {
        return engineFactory;
    }

    public void setEngine(IClusteringEngine engine) {
        this.engine = engine;
        updateCache();
    }

}
