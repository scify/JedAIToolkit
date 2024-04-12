package fragmentation;

import fragmentation.finalalgorithms.*;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.graph.ConnectedComponents;
import org.scify.jedai.utilities.graph.UndirectedGraph;

/**
 *
 * @author Georgios
 */
public class Algorithm2 {

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

    static double getEntityClusterSim(SimilarityFunction simFunction, TIntSet entityItems, TIntSet clusterItemss) {
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

        int skippedNVPairs = 0;
        final Set<String> attributesD1 = new HashSet<>();
        for (EntityProfile p : profiles1) {
            for (Attribute a : p.getAttributes()) {
                if (a.getName().equals("title") || a.getName().equals("name")) {
                    skippedNVPairs++;
                    continue;
                }

                attributesD1.add(a.getName().toLowerCase());
            }
        }
        System.out.println("Skipped NV pairs\t:\t" + skippedNVPairs);

        skippedNVPairs = 0;
        final Set<String> attributesD2 = new HashSet<>();
        for (EntityProfile p : profiles2) {
            for (Attribute a : p.getAttributes()) {
                if (a.getName().equals("title") || a.getName().equals("name")) {
                    skippedNVPairs++;
                    continue;
                }

                attributesD2.add(a.getName().toLowerCase());
            }
        }
        System.out.println("Skipped NV pairs\t:\t" + skippedNVPairs);

        final Set<String> commonAttributes = new HashSet<>(attributesD1);
        commonAttributes.retainAll(attributesD2);
        System.out.println("Common attributes\t:\t" + commonAttributes.size());

        String[] attributes = new String[commonAttributes.size()];
        attributes = commonAttributes.toArray(attributes);

        final Map<String, Integer> attributeIds = new HashMap<>();
        for (int i = 0; i < attributes.length; i++) {
            attributeIds.put(attributes[i], i);
        }

        final List<EntityProfile> allProfiles = new ArrayList<>();
        allProfiles.addAll(profiles1);
        allProfiles.addAll(profiles2);

        int[][] coOccurrenceMatrix = new int[attributes.length][attributes.length];
        for (EntityProfile p : allProfiles) {
            final TIntSet localAttributes = new TIntHashSet();
            for (Attribute a : p.getAttributes()) {
                Integer attrId = attributeIds.get(a.getName().toLowerCase());
                if (attrId != null) {
                    localAttributes.add(attrId);
                }
            }

            final TIntList sortedIds = new TIntArrayList(localAttributes);
            sortedIds.sort();

            int[] attrArray = sortedIds.toArray();
            for (int i = 0; i < attrArray.length; i++) {
                for (int j = i; j < attrArray.length; j++) { // we also increase the diagonals
                    coOccurrenceMatrix[attrArray[i]][attrArray[j]]++;
                }
            }
        }

        for (double threshold = 0.05; threshold < 1.0; threshold += 0.05) {
            System.out.println("\n\nCurrent threshold\t:\t" + threshold);

            final UndirectedGraph similarityGraph = new UndirectedGraph(commonAttributes.size());
            for (int i = 0; i < attributes.length; i++) {
                for (int j = i + 1; j < attributes.length; j++) {
                    if (0 < coOccurrenceMatrix[i][j]) {
                        double sim = getItemsSim(simFunction, coOccurrenceMatrix[i][j], coOccurrenceMatrix[i][i], coOccurrenceMatrix[j][j]);
                        if (threshold < sim) {
                            similarityGraph.addEdge(i, j);
                        }
                    }
                }
            }

            final ConnectedComponents cc = new ConnectedComponents(similarityGraph);

            final TIntSet[] clusterAttributes = new TIntHashSet[cc.count() + 1];
            final TIntSet[] d1ProfilesPerCluster = new TIntSet[cc.count() + 1];
            final TIntSet[] d2ProfilesPerCluster = new TIntSet[cc.count() + 1];
            System.out.println("Total clusters\t:\t" + cc.count());
            for (int i = 0; i <= cc.count(); i++) {
                clusterAttributes[i] = new TIntHashSet();
                d1ProfilesPerCluster[i] = new TIntHashSet();
                d2ProfilesPerCluster[i] = new TIntHashSet();
            }

            int[] attributeToClusterId = new int[commonAttributes.size()]; // can be removed!
            for (int i = 0; i < attributes.length; i++) {
                attributeToClusterId[i] = cc.id(i);
                clusterAttributes[cc.id(i)].add(i);
            }

            int counter = 0;
            int glueId = cc.count();
            for (EntityProfile p : profiles1) {
                final TIntSet entityClusters = new TIntHashSet();
                final TIntSet entityAttributes = new TIntHashSet();
                for (Attribute a : p.getAttributes()) {
                    Integer attributeId = attributeIds.get(a.getName().toLowerCase());
                    if (attributeId == null) {
                        continue;
                    }
                    int clusterId = attributeToClusterId[attributeId];
                    entityClusters.add(clusterId);
                    entityAttributes.add(attributeId);
                }

                int bestId = -1;
                double maxSim = -1;
                final TIntIterator iterator = entityClusters.iterator();
                while (iterator.hasNext()) {
                    int clusterId = iterator.next();
                    double sim = getEntityClusterSim(simFunction, entityAttributes, clusterAttributes[clusterId]);
                    if (maxSim < sim) {
                        bestId = clusterId;
                        maxSim = sim;
                    }
                }
                if (-1 < bestId) {
                    d1ProfilesPerCluster[bestId].add(counter);
                } else {
                    d1ProfilesPerCluster[glueId].add(counter);
                }

                counter++;
            }

            counter = 0;
            for (EntityProfile p : profiles2) {
                final TIntSet entityClusters = new TIntHashSet();
                final TIntSet entityAttributes = new TIntHashSet();
                for (Attribute a : p.getAttributes()) {
                    Integer attributeId = attributeIds.get(a.getName().toLowerCase());
                    if (attributeId == null) {
                        continue;
                    }
                    int clusterId = attributeToClusterId[attributeId];
                    entityClusters.add(clusterId);
                    entityAttributes.add(attributeId);
                }

                int bestId = -1;
                double maxSim = -1;
                final TIntIterator iterator = entityClusters.iterator();
                while (iterator.hasNext()) {
                    int clusterId = iterator.next();
                    double sim = getEntityClusterSim(simFunction, entityAttributes, clusterAttributes[clusterId]);
                    if (maxSim < sim) {
                        bestId = clusterId;
                        maxSim = sim;
                    }
                }
                if (-1 < bestId) {
                    d2ProfilesPerCluster[bestId].add(counter);
                } else {
                    d2ProfilesPerCluster[glueId].add(counter);
                }
                counter++;
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
