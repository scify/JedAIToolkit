package fragmentation;

import fragmentation.configurations.*;
import java.util.List;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.entityclustering.UniqueMappingClustering;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;

/**
 *
 * @author Georgios
 */
public abstract class AbstractBestConfiguration {

    protected long blockingTime;
    protected long matchingTime;

    protected final AbstractDuplicatePropagation duplicatePropagation;
    protected BlocksPerformance blocksPerformance;
    protected ClustersPerformance matchingPerformance;
    protected List<AbstractBlock> blocks;
    protected final List<EntityProfile> profiles1;
    protected final List<EntityProfile> profiles2;
    protected String blockingConfiguration;
    protected String blockingName;
    protected String matchingConfiguration;
    protected String matchingName;
    protected SimilarityPairs simPairs;
    protected UniqueMappingClustering umc;

    public AbstractBestConfiguration(AbstractDuplicatePropagation dp, List<EntityProfile> pf1, List<EntityProfile> pf2) {
        profiles1 = pf1;
        profiles2 = pf2;
        duplicatePropagation = dp;
        blockingConfiguration = "";
        blockingName = "";
        matchingConfiguration = "";
        matchingName = "";
    }

    public abstract void applyWorkflow();

    public double getF1() {
        return matchingPerformance.getFMeasure();
    }

    public EquivalenceCluster[] getMatches() {
        return matchingPerformance.getMatches();
    }

    protected void setBlockingPerformance() {
        blocksPerformance = new BlocksPerformance(blocks, duplicatePropagation);
        blocksPerformance.setStatistics();
    }

    protected void setMatchingPerformance() {
        final EquivalenceCluster[] entityClusters = umc.getDuplicates(simPairs);
        matchingPerformance = new ClustersPerformance(entityClusters, duplicatePropagation);
        matchingPerformance.setStatistics();
        matchingPerformance.printStatistics(0, "", "");
    }

    public void printPerformance() {
        if (blocksPerformance == null) {
            System.err.println("Blocking performance has not been set!");
        } else {
            blocksPerformance.printStatistics(blockingTime, blockingName, blockingConfiguration);
        }
        
        if (matchingPerformance == null) {
            System.err.println("Performance has not been set!");
            return;
        }
        matchingPerformance.printStatistics(matchingTime, matchingName, matchingConfiguration);
    }
}
