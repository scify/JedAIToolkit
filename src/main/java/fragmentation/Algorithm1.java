/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fragmentation;

import fragmentation.finalalgorithms.*;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
public class Algorithm1 {

    static List<String> getAllLines(String fileName) throws IOException {
        final List<String> lines = new ArrayList<>();
        final BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line = reader.readLine();
        lines.add(line);
        while (line != null) {
            line = reader.readLine();
            if (line == null) {
                continue;
            }
            String name = line.trim();
            if (name.length() == 0) {
                continue;
            }
            lines.add(name);
        }
        reader.close();
        return lines;
    }

    public static void main(String[] args) throws IOException {
        int embeddingsId = Integer.parseInt(args[0]);
        System.out.println("Embeddings id\t:\t" + embeddingsId);

        int sizeThreshold = Integer.parseInt(args[1]);
        System.out.println("Size threshold\t:\t" + sizeThreshold);

        String datasetsD1 = "newDBPedia1";
        String datasetsD2 = "newDBPedia2";
        String mainDir = "/home/servers/matcherSelection/data/";

        String filePath1 = null;
        String filePath2 = null;
        switch (embeddingsId) {
            case 0:
                filePath1 = mainDir + "fastText/DBPediaNN10.csv";
                filePath2 = mainDir + "fastText/DBPediaDist10.csv";
                break;
            case 1:
                filePath1 = mainDir + "sminilm/DBPediaNN10.csv";
                filePath2 = mainDir + "sminilm/DBPediaDist10.csv";
                break;
            case 2:
                filePath1 = mainDir + "sminilm/DBPediaNN10cosineIVF.csv";
                filePath2 = mainDir + "sminilm/DBPediaDist10cosineIVF.csv";
                break;
        }
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

        final List<String> linesNeighbors = getAllLines(filePath1);
        final List<String> linesDistances = getAllLines(filePath2);

        int N = 10;
        int entitiesD1 = 1190733;
        int entitiesD2 = 2164040;

        for (double threshold = 0.05; threshold < 1.0; threshold += 0.05) {
            System.out.println("\n\nCurrent threshold\t:\t" + threshold);

            int entityTypes = 0;
            int realMatches = 0;
            double maxComps = 0;
            double totalPairs = 0;

            UndirectedGraph similarityGraph = new UndirectedGraph(entitiesD1 + entitiesD2);
            for (int i = 0; i < entitiesD1; i++) {
                String neighbors = linesNeighbors.get(i);
                String[] nTokens = neighbors.split("\\|");

                String distances = linesDistances.get(i);
                String[] dTokens = distances.split("\\|");

                for (int j = 0; j < N; j++) {
                    int currentId = Integer.parseInt(nTokens[j]);
                    int normalizedId = entitiesD1 + currentId;

                    double currentSim = -1;
                    switch (embeddingsId) {
                        case 0:
                        case 1:
                            currentSim = 1 / (1 + Double.parseDouble(dTokens[j]));
                            break;
                        case 2:
                            currentSim = Double.parseDouble(dTokens[j]);
                            break;
                    }

                    if (threshold < currentSim) {
                        similarityGraph.addEdge(i, normalizedId);
                    }
                }
            }

            final ConnectedComponents cc = new ConnectedComponents(similarityGraph);

            final TIntSet d1ClusterIds = new TIntHashSet();
            final double[] clusterFrequencyD1 = new double[cc.count()];
            for (int i = 0; i < entitiesD1; i++) {
                int ccId = cc.id(i);
                d1ClusterIds.add(ccId);
                clusterFrequencyD1[ccId]++;
            }

            final TIntSet d2ClusterIds = new TIntHashSet();
            final double[] clusterFrequencyD2 = new double[cc.count()];
            for (int i = entitiesD1; i < entitiesD1 + entitiesD2; i++) {
                int ccId = cc.id(i);
                d2ClusterIds.add(ccId);
                clusterFrequencyD2[ccId]++;
            }

            final TIntSet commonClusters = new TIntHashSet(d1ClusterIds);
            commonClusters.retainAll(d2ClusterIds);

            final TIntSet validClusters = new TIntHashSet();
            TIntIterator clusterIterator = commonClusters.iterator();
            while (clusterIterator.hasNext()) {
                int clusterId = clusterIterator.next();
                double currentComps = clusterFrequencyD1[clusterId] * clusterFrequencyD2[clusterId];
                if (sizeThreshold < currentComps) {
                    totalPairs += currentComps;
                    if (maxComps < currentComps) {
                        maxComps = currentComps;
                    }
                    entityTypes++;
                    validClusters.add(clusterId);
                }
            }

            int d1GlueCluster = 0;
            final int[] d1EntitiesCluster = new int[entitiesD1];
            for (int i = 0; i < entitiesD1; i++) {
                int ccId = cc.id(i);
                if (validClusters.contains(ccId)) {
                    d1EntitiesCluster[i] = ccId;
                } else {
                    d1GlueCluster++;
                    d1EntitiesCluster[i] = -1;
                }
            }

            int d2GlueCluster = 0;
            final int[] d2EntitiesCluster = new int[entitiesD2];
            for (int i = entitiesD1; i < entitiesD1 + entitiesD2; i++) {
                int ccId = cc.id(i);
                if (validClusters.contains(ccId)) {
                    d2EntitiesCluster[i - entitiesD1] = ccId;
                } else {
                    d2GlueCluster++;
                    d2EntitiesCluster[i - entitiesD1] = -1;
                }
            }

            double glueComparisons = ((double) d1GlueCluster) * d2GlueCluster;
            System.out.println("Glue cluster pairs\t:\t" + glueComparisons);
            if (maxComps < glueComparisons) {
                maxComps = glueComparisons;
            }
            if (0 < glueComparisons) {
                entityTypes++;
            }
            totalPairs += glueComparisons;

            for (IdDuplicates pair : duplicates) {
                if (d1EntitiesCluster[pair.getEntityId1()] == d2EntitiesCluster[pair.getEntityId2()]) {
//                if (0 <= d1EntitiesCluster[pair.getEntityId1()]
//                        && 0 <= d2EntitiesCluster[pair.getEntityId2()]
//                        && d1EntitiesCluster[pair.getEntityId1()] == d2EntitiesCluster[pair.getEntityId2()]) {
                    realMatches++;
                }
            }

            System.out.println("Valid clusters\t:\t" + entityTypes);
            System.out.println("Total comparisons\t:\t" + totalPairs);
            System.out.println("Maximum comparisons\t:\t" + maxComps);
            System.out.println("PC=" + ((double) realMatches / duplicates.size()));
            System.out.println("PQ=" + (realMatches / totalPairs));
        }
    }
}
