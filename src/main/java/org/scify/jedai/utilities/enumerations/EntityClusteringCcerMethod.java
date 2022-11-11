/*
* Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */
package org.scify.jedai.utilities.enumerations;

import org.scify.jedai.entityclustering.*;

/**
 *
 * @author GAP2
 */
public enum EntityClusteringCcerMethod {
    UNIQUE_MAPPING_CLUSTERING,
    ROW_COLUMN_ASSIGNMENT_CLUSTERING,
    ROW_COLUMN_ASSIGNMENT_CLUSTERING_ONLY_EDGES,
    ROW_COLUMN_ASSIGNMENT_CLUSTERING_ONLY_EDGES_EFFICIENT,
    BEST_ASSIGNMENT_HEURISTIC_CLUSTERING,
    BEST_ASSIGNMENT_HEURISTIC_CLUSTERING_EFFICIENT,
    BAH_AFTER_UM,
    BAH_AFTER_UM_EFFICIENT,
    CONNECTED_COMPONENTS_CCER,
    BEST_MATCH_CLUSTERING,
    SYMMETRIC_BEST_MATCH_CLUSTERING,
    RICOCHETSR_CLUSTERING_CCER,
    KIRALY_CLUSTERING;

    public static IEntityClustering getDefaultConfiguration(EntityClusteringCcerMethod ecMethod) {
        switch (ecMethod) {
            case UNIQUE_MAPPING_CLUSTERING:
                return new UniqueMappingClustering();
            case ROW_COLUMN_ASSIGNMENT_CLUSTERING:
                return new RowColumnClustering();
            case ROW_COLUMN_ASSIGNMENT_CLUSTERING_ONLY_EDGES:
                return new RowColumnClusteringOnlyEdges();
            case ROW_COLUMN_ASSIGNMENT_CLUSTERING_ONLY_EDGES_EFFICIENT:
                return new EfficientRowColumnClusteringOnlyEdges();
            case BEST_ASSIGNMENT_HEURISTIC_CLUSTERING:
                return new BestAssignmentHeuristic();
            case BEST_ASSIGNMENT_HEURISTIC_CLUSTERING_EFFICIENT:
                return new EfficientBestAssignmentHeuristic();
            case BAH_AFTER_UM:
                return new BAHafterUM();
            case BAH_AFTER_UM_EFFICIENT:
                return new EfficientBAHafterUM();
            case CONNECTED_COMPONENTS_CCER:
                return new ConnectedComponentsClusteringCCER();
            case BEST_MATCH_CLUSTERING:
                return new BestMatchClustering();
            case SYMMETRIC_BEST_MATCH_CLUSTERING:
                return new ExactClustering();
            case RICOCHETSR_CLUSTERING_CCER:
                return new RicochetSRClusteringCCER();
            case KIRALY_CLUSTERING:
                return new KiralyMSMApproxClustering();
            default:
                return new UniqueMappingClustering();
        }
    }
}
