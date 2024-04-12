/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fragmentation;

import java.util.List;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;

/**
 *
 * @author Georgios
 */
public enum BestConfiguration {
    CONF_1,
    CONF_2,
    CONF_3,
    CONF_4,
    CONF_5,
    CONF_6,
    CONF_7,
    CONF_8,
    CONF_9,
    CONF_10;

    public static AbstractBestConfiguration getBestConfiguration(AbstractDuplicatePropagation dp, BestConfiguration bc, List<EntityProfile> pf1, List<EntityProfile> pf2) {
        switch (bc) {
            case CONF_1:
                return new BestPipelineForD1(dp, pf1, pf2);
            case CONF_2:
                return new BestPipelineForD2(dp, pf1, pf2);
            case CONF_3:
                return new BestPipelineForD3(dp, pf1, pf2);
            case CONF_4:
                return new BestPipelineForD4(dp, pf1, pf2);
            case CONF_5:
                return new BestPipelineForD5(dp, pf1, pf2);
            case CONF_6:
                return new BestPipelineForD6(dp, pf1, pf2);
            case CONF_7:
                return new BestPipelineForD7(dp, pf1, pf2);
            case CONF_8:
                return new BestPipelineForD8(dp, pf1, pf2);
            case CONF_9:
                return new BestPipelineForD9(dp, pf1, pf2);
            case CONF_10:
                return new BestPipelineForD10(dp, pf1, pf2);
        }
        return null;
    }

}
