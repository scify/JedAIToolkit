package org.scify.jedai.version3;

import test.*;
import java.util.List;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntityCSVReader;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

/**
 *
 * @author Georgios
 */
public class TestSongsDataset {

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

            final StandardBlocking sb = new StandardBlocking();
            List<AbstractBlock> blocks = sb.getBlocks(profiles);

            double comparisons = 0;
            for (AbstractBlock b : blocks) {
                comparisons += b.getNoOfComparisons();
            }
            System.out.println("BB blocks\t:\t" + blocks.size());
            System.out.println("BB comparisons\t:\t" + comparisons);

            final ComparisonsBasedBlockPurging cbbp = new ComparisonsBasedBlockPurging(false);
            blocks = cbbp.refineBlocks(blocks);

            comparisons = 0;
            for (AbstractBlock b : blocks) {
                comparisons += b.getNoOfComparisons();
            }
            System.out.println("BC blocks\t:\t" + blocks.size());
            System.out.println("BC comparisons\t:\t" + comparisons);

            final IBlockProcessing blockFiltering = new BlockFiltering();
            blocks = blockFiltering.refineBlocks(blocks);

            comparisons = 0;
            for (AbstractBlock b : blocks) {
                comparisons += b.getNoOfComparisons();
            }
            System.out.println("BF blocks\t:\t" + blocks.size());
            System.out.println("BF comparisons\t:\t" + comparisons);

            final IBlockProcessing comparisonCleaningMethod = new CardinalityNodePruning(WeightingScheme.JS);
            blocks = comparisonCleaningMethod.refineBlocks(blocks);

            comparisons = 0;
            for (AbstractBlock b : blocks) {
                comparisons += b.getNoOfComparisons();
            }
            System.out.println("CC blocks\t:\t" + blocks.size());
            System.out.println("CC comparisons\t:\t" + comparisons);
        }
    }
}
