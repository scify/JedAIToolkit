package fragmentation;

import fragmentation.finalalgorithms.*;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.BilateralBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.BlockBuildingMethod;
import org.scify.jedai.utilities.graph.ConnectedComponents;
import org.scify.jedai.utilities.graph.UndirectedGraph;

/**
 *
 * @author Georgios
 */
public class Algorithm3 {

    static double getItemsSim(SimilarityFunction simFunction, double commonFreq, double frequency1, double frequency2) {
        switch (simFunction) {
            case COSINE_SIM:
                return commonFreq / Math.sqrt(frequency1 * frequency2);
            case DICE_SIM:
                return 2 * commonFreq / (frequency1 + frequency2);
            case JACCARD_SIM:
                return commonFreq / (frequency1 + frequency2 - commonFreq);
        }
        return -1;
    }

    static double getEntityClusterSim(SimilarityFunction simFunction, TIntList entityItems, TIntSet clusterItemss) {
        final TIntSet commonIds = new TIntHashSet(entityItems);
        commonIds.retainAll(clusterItemss);

        switch (simFunction) {
            case COSINE_SIM:
                return ((double) commonIds.size()) / Math.sqrt(entityItems.size() * clusterItemss.size());
            case DICE_SIM:
                return 2 * ((double) commonIds.size()) / (entityItems.size() + clusterItemss.size());
            case JACCARD_SIM:
                return ((double) commonIds.size()) / (entityItems.size() + clusterItemss.size() - commonIds.size());
        }
        return -1;
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();

        int sizeThreshold = Integer.parseInt(args[0]);
        System.out.println("Size threshold\t:\t" + sizeThreshold);
        int simFunctionId = Integer.parseInt(args[1]);
        SimilarityFunction simFunction = SimilarityFunction.values()[simFunctionId];
        System.out.println("Similarity function\t:\t" + simFunction);

        String mainDir = "/home/servers/matcherSelection/data/";
        String datasetsD1 = "newDBPedia1";
        String datasetsD2 = "newDBPedia2";
        String groundtruthDirs = "newDBPediaMatches";

        final IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1);
        final List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
        System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

        final IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2);
        final List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
        System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

        final IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs);
        final Set<IdDuplicates> duplicates = gtReader.getDuplicatePairs(null);
        System.out.println("Existing Duplicates\t:\t" + duplicates.size());

        IBlockBuilding blockBuildingMethod = BlockBuildingMethod.getDefaultConfiguration(BlockBuildingMethod.STANDARD_BLOCKING);
        List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profiles1, profiles2);

        final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(true);
        blocks = cbbp.refineBlocks(blocks);

        final BlockFiltering bf = new BlockFiltering(0.5f);
        blocks = bf.refineBlocks(blocks);

        BlocksPerformance blStats = new BlocksPerformance(blocks, new BilateralDuplicatePropagation(duplicates));
        blStats.setStatistics();
        blStats.printStatistics(0, "", "");

        int datasetLimit = profiles1.size();
        int noOfFeatures = blocks.size();
        int totalEntities = profiles1.size() + profiles2.size();

        // get common terms and entity ids per term
        int counter = 0;
        int[][] arrayOfBlocks = new int[blocks.size()][];
        for (AbstractBlock block : blocks) {
            // sort entity ids in block
            final TIntList entityIds = new TIntArrayList();
            for (int id : ((BilateralBlock) block).getIndex1Entities()) {
                entityIds.add(id);
            }
            for (int id : ((BilateralBlock) block).getIndex2Entities()) {
                entityIds.add(id + datasetLimit);
            }
            entityIds.sort();
            arrayOfBlocks[counter++] = entityIds.toArray();
        }

        // index from entity ids to block/term ids
        final TIntList[] invertedIndex = new TIntArrayList[totalEntities];
        for (int i = 0; i < noOfFeatures; i++) {
            for (int entityId : arrayOfBlocks[i]) {
                if (invertedIndex[entityId] == null) {
                    invertedIndex[entityId] = new TIntArrayList();
                }
                invertedIndex[entityId].add(i);
            }
        }

        for (double threshold = 0.05; threshold < 1.0; threshold += 0.05) {
            System.out.println("\n\nCurrent threshold\t:\t" + threshold);

            final UndirectedGraph graph = new UndirectedGraph(noOfFeatures);

            for (int i = 0; i < noOfFeatures; i++) {
                final TIntSet candidates = new TIntHashSet();
                for (int entityId : arrayOfBlocks[i]) {
                    candidates.addAll(invertedIndex[entityId]);
                }

                TIntIterator iterator = candidates.iterator();
                while (iterator.hasNext()) {
                    int j = iterator.next();
                    if (i <= j) {
                        continue;
                    }

                    final TIntList commonBlocks = new TIntArrayList(arrayOfBlocks[i]);
                    commonBlocks.retainAll(arrayOfBlocks[j]);
                    double noOfCommonEntities = commonBlocks.size();
                    if (0 < noOfCommonEntities) {
                        double sim = getItemsSim(simFunction, noOfCommonEntities, arrayOfBlocks[j].length, arrayOfBlocks[i].length);
                        if (threshold < sim) {
                            graph.addEdge(i, j);
                        }
                    }
                }
            }

            final ConnectedComponents cc = new ConnectedComponents(graph);

            final TIntSet[] clusterTerms = new TIntHashSet[cc.count() + 1];
            final TIntSet[] d1ProfilesPerCluster = new TIntSet[cc.count() + 1];
            final TIntSet[] d2ProfilesPerCluster = new TIntSet[cc.count() + 1];
            System.out.println("Total clusters\t:\t" + cc.count());
            for (int i = 0; i <= cc.count(); i++) {
                clusterTerms[i] = new TIntHashSet();
                d1ProfilesPerCluster[i] = new TIntHashSet();
                d2ProfilesPerCluster[i] = new TIntHashSet();
            }

            for (int i = 0; i < noOfFeatures; i++) {
                clusterTerms[cc.id(i)].add(i);
            }

            int glueId = cc.count();
            
            for (int i = 0; i < datasetLimit; i++) {
                final TIntSet entityClusters = new TIntHashSet();
                if  (invertedIndex[i] == null) {
                    continue;
                }
                
                TIntIterator iterator = invertedIndex[i].iterator();
                while (iterator.hasNext()) {
                    int itemId = iterator.next();
                    int clusterId = cc.id(itemId);
                    entityClusters.add(clusterId);
                }

                int bestId = -1;
                double maxSim = -1;
                iterator = entityClusters.iterator();
                while (iterator.hasNext()) {
                    int clusterId = iterator.next();
                    double sim = getEntityClusterSim(simFunction, invertedIndex[i], clusterTerms[clusterId]);
                    if (maxSim < sim) {
                        bestId = clusterId;
                        maxSim = sim;
                    }
                }
                if (-1 < bestId) {
                    d1ProfilesPerCluster[bestId].add(i);
                } else {
                    d1ProfilesPerCluster[glueId].add(i);
                }
            }

            for (int i = datasetLimit; i < totalEntities; i++) {
                final TIntSet entityClusters = new TIntHashSet();
                if  (invertedIndex[i] == null) {
                    continue;
                }
                
                TIntIterator iterator = invertedIndex[i].iterator();
                while (iterator.hasNext()) {
                    int itemId = iterator.next();
                    int clusterId = cc.id(itemId);
                    entityClusters.add(clusterId);
                }
                
                int bestId = -1;
                double maxSim = -1;
                iterator = entityClusters.iterator();
                while (iterator.hasNext()) {
                    int clusterId = iterator.next();
                    double sim = getEntityClusterSim(simFunction, invertedIndex[i], clusterTerms[clusterId]);
                    if (maxSim < sim) {
                        bestId = clusterId;
                        maxSim = sim;
                    }
                }
                if (-1 < bestId) {
                    d2ProfilesPerCluster[bestId].add(i - datasetLimit);
                } else {
                    d2ProfilesPerCluster[glueId].add(i - datasetLimit);
                }
            }

            int validClusters = 0;
            float totalComparisons = 0;
            final List<Float> comparisonsPerCluster = new ArrayList<>();
            final int[] entitiesD1ToCluster = new int[profiles1.size()];
            for (int i = 0; i < entitiesD1ToCluster.length; i++) {
                entitiesD1ToCluster[i] = -1;
            }
            final int[] entitiesD2ToCluster = new int[profiles2.size()];
            for (int i = 0; i < entitiesD2ToCluster.length; i++) {
                entitiesD2ToCluster[i] = -1;
            }
            for (int i = 0; i < d1ProfilesPerCluster.length - 1; i++) { // all except glue cluster
                float currentComps = ((float) d1ProfilesPerCluster[i].size()) * d2ProfilesPerCluster[i].size();
                if (sizeThreshold < currentComps) {
                    validClusters++;
                    totalComparisons += currentComps;
                    comparisonsPerCluster.add(currentComps);

                    TIntIterator iterator = d1ProfilesPerCluster[i].iterator();
                    while (iterator.hasNext()) {
                        int entityId1 = iterator.next();
                        entitiesD1ToCluster[entityId1] = i;
                    }

                    iterator = d2ProfilesPerCluster[i].iterator();
                    while (iterator.hasNext()) {
                        int entityId2 = iterator.next();
                        entitiesD2ToCluster[entityId2] = i;
                    }
                } else {
                    d1ProfilesPerCluster[glueId].addAll(d1ProfilesPerCluster[i]);
                    d2ProfilesPerCluster[glueId].addAll(d2ProfilesPerCluster[i]);
                }
            }
            //process glue cluster
            float currentComps = ((float) d1ProfilesPerCluster[glueId].size()) * d2ProfilesPerCluster[glueId].size();
            if (sizeThreshold < currentComps) {
                validClusters++;
                totalComparisons += currentComps;
                comparisonsPerCluster.add(currentComps);

                TIntIterator iterator = d1ProfilesPerCluster[glueId].iterator();
                while (iterator.hasNext()) {
                    int entityId1 = iterator.next();
                    entitiesD1ToCluster[entityId1] = glueId;
                }

                iterator = d2ProfilesPerCluster[glueId].iterator();
                while (iterator.hasNext()) {
                    int entityId2 = iterator.next();
                    entitiesD2ToCluster[entityId2] = glueId;
                }
            }

            System.out.println("Total comparisons\t:\t" + totalComparisons);
            System.out.println("Total valid clusters\t:\t" + validClusters);
            System.out.println("Total fragments\t:\t" + comparisonsPerCluster.size());

            double meanValue = StatisticsUtilities.getMeanValue(comparisonsPerCluster);
            System.out.println("Minimum comparisons\t:\t" + StatisticsUtilities.getMinValue(comparisonsPerCluster));
            System.out.println("First Quartile\t:\t" + StatisticsUtilities.getFirstQuartile(comparisonsPerCluster));
            System.out.println("Median comparisons\t:\t" + StatisticsUtilities.getMedianValue(comparisonsPerCluster));
            System.out.println("Mean comparisons\t:\t" + meanValue);
            System.out.println("StDev comparisons\t:\t" + StatisticsUtilities.getStandardDeviation(meanValue, comparisonsPerCluster));
            System.out.println("Third Quartile\t:\t" + StatisticsUtilities.getThirdQuartile(comparisonsPerCluster));
            System.out.println("Maximum comparisons\t:\t" + StatisticsUtilities.getMaxValue(comparisonsPerCluster));

            double missingDuplicates = 0;
            double detectedDuplicates = 0;
            for (IdDuplicates pair : duplicates) {
                if (entitiesD1ToCluster[pair.getEntityId1()] < 0
                        || entitiesD2ToCluster[pair.getEntityId2()] < 0) {
                    missingDuplicates++;
                    continue;
                }

                if (entitiesD1ToCluster[pair.getEntityId1()] == entitiesD2ToCluster[pair.getEntityId2()]) {
                    detectedDuplicates++;
                }
            }
            System.out.println("Missing duplicates\t:\t" + missingDuplicates);
            System.out.println("Detected duplicates\t:\t" + detectedDuplicates);

            double pc = detectedDuplicates / duplicates.size();
            double pq = detectedDuplicates / totalComparisons;
            double f1 = 2 * pc * pq / (pc + pq);
            System.out.println("PC=" + pc);
            System.out.println("PQ=" + pq);
            System.out.println("F1=" + f1);
        }
    }
}
