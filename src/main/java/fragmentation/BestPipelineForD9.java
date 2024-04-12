package fragmentation;

import java.util.List;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.AbstractMetablocking;
import org.scify.jedai.blockprocessing.comparisoncleaning.ReciprocalCardinalityNodePruning;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.entityclustering.UniqueMappingClustering;
import org.scify.jedai.entitymatching.ProfileMatcher;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

/**
 *
 * @author Georgios
 */
public class BestPipelineForD9 extends AbstractBestConfiguration {

    public BestPipelineForD9(AbstractDuplicatePropagation dp, List<EntityProfile> pf1, List<EntityProfile> pf2) {
        super(dp, pf1, pf2);
    }

    @Override
    public void applyWorkflow() {
        long time1 = System.currentTimeMillis();

        final StandardBlocking sb = new StandardBlocking();
        blocks = sb.getBlocks(profiles1, profiles2);

        final BlockFiltering bf = new BlockFiltering();
        bf.setNumberedGridConfiguration(32);
        blocks = bf.refineBlocks(blocks);

        final AbstractMetablocking mb = new ReciprocalCardinalityNodePruning();
        mb.setNumberedGridConfiguration(1);
        blocks = mb.refineBlocks(blocks);

//        setBlockingPerformance();
        long time2 = System.currentTimeMillis();

        final ProfileMatcher pm = new ProfileMatcher(profiles1, profiles2,
                RepresentationModel.CHARACTER_FOURGRAMS_TF_IDF, SimilarityMetric.COSINE_SIMILARITY);
        simPairs = pm.executeComparisons(blocks);

        umc = new UniqueMappingClustering();
        umc.setNumberedGridConfiguration(5);

        long time3 = System.currentTimeMillis();
        blockingTime = time2 - time1;
        matchingTime = time3 - time2;
        
        setMatchingPerformance();
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();

        String mainDir = "/home/servers/matcherSelection/data/";
        String[] datasetsD1 = {"restaurant1Profiles", "abtProfiles", "amazonProfiles", "dblpProfiles", "imdbProfilesNEW", "imdbProfilesNEW", "tmdbProfiles", "walmartProfiles", "dblpProfiles2", "imdbProfiles"};
        String[] datasetsD2 = {"restaurant2Profiles", "buyProfiles", "gpProfiles", "acmProfiles", "tmdbProfiles", "tvdbProfiles", "tvdbProfiles", "amazonProfiles2", "scholarProfiles", "dbpediaProfiles"};
        String[] groundtruthDirs = {"restaurantsIdDuplicates", "abtBuyIdDuplicates", "amazonGpIdDuplicates", "dblpAcmIdDuplicates", "imdbTmdbIdDuplicates", "imdbTvdbIdDuplicates", "tmdbTvdbIdDuplicates", "amazonWalmartIdDuplicates",
            "dblpScholarIdDuplicates", "moviesIdDuplicates"};

        for (int datasetId = 0; datasetId < datasetsD1.length; datasetId++) {
            System.out.println("\n\n\nCurrent dataset\t:\t" + groundtruthDirs[datasetId]);

            IEntityReader eReader1 = new EntitySerializationReader(mainDir + datasetsD1[datasetId]);
            List<EntityProfile> profiles1 = eReader1.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles1.size());

            IEntityReader eReader2 = new EntitySerializationReader(mainDir + datasetsD2[datasetId]);
            List<EntityProfile> profiles2 = eReader2.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles2.size());

            IGroundTruthReader gtReader = new GtSerializationReader(mainDir + groundtruthDirs[datasetId]);
            final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(null));
            System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

            final BestPipelineForD9 bf1d9 = new BestPipelineForD9(duplicatePropagation, profiles1, profiles2);
            bf1d9.applyWorkflow();
            bf1d9.printPerformance();
        }
    }
}
