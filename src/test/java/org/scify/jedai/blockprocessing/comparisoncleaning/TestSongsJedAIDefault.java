package org.scify.jedai.blockprocessing.comparisoncleaning;

import java.util.List;
import org.scify.jedai.blockbuilding.QGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntityCSVReader;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author Georgios
 */
public class TestSongsJedAIDefault {

    private static void getStatistics(long rt, List<AbstractBlock> blocks, String prefix) {
        double comparisons = 0;
        for (AbstractBlock b : blocks) {
            comparisons += b.getNoOfComparisons();
        }
        System.out.println(prefix + " run-time\t:\t" + rt);
        System.out.println(prefix + " blocks\t:\t" + blocks.size());
        System.out.println(prefix + " comparisons\t:\t" + comparisons);
    }

    public static void main(String[] args) {
        String dir = "/home/gpapadakis/data/";
        String[] filepath = {dir + "songs_clean.csv",
            dir + "songs_dirty.csv",};

        for (String file : filepath) {
            System.out.println("\n\n\n\n\nCurrent file path\t:\t" + file);

            EntityCSVReader reader = new EntityCSVReader(file);
            reader.setAttributeNamesInFirstRow(true);
            reader.setIdIndex(0);
            reader.setAttributesToExclude(new int[]{2, 4, 5}); // keep only id, title and artist
            List<EntityProfile> profiles = reader.getEntityProfiles();
            System.out.println("Loaded profiles\t:\t" + profiles.size());

//            for (EntityProfile profile : profiles) {
//                System.out.println(profile.toString());
//            }

            long time1 = System.currentTimeMillis();

            final QGramsBlocking qb = new QGramsBlocking(6);
            List<AbstractBlock> blocks = qb.getBlocks(profiles);

            long time2 = System.currentTimeMillis();

            getStatistics(time2 - time1, blocks, "BB");

            long time3 = System.currentTimeMillis();

            final BlockFiltering bf = new BlockFiltering(0.50f);
            blocks = bf.refineBlocks(blocks);

            long time4 = System.currentTimeMillis();

            getStatistics(time4 - time3, blocks, "BF");

            long time5 = System.currentTimeMillis();

           final IBlockProcessing comparisonCleaningMethod = new WeightedEdgePruning(WeightingScheme.ECBS);
            blocks = comparisonCleaningMethod.refineBlocks(blocks);

            long time6 = System.currentTimeMillis();

            getStatistics(time6 - time5, blocks, "CC");
        }
    }
}
