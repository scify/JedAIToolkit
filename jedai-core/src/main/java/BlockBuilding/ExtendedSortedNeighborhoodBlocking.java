/*
 * Copyright [2016-2017] [George Papadakis (gpapadis@yahoo.gr)]
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
package BlockBuilding;

import DataModel.BilateralBlock;
import DataModel.UnilateralBlock;

import gnu.trove.list.TIntList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 *
 * @author gap2
 */
public class ExtendedSortedNeighborhoodBlocking extends SortedNeighborhoodBlocking {

    public ExtendedSortedNeighborhoodBlocking() {
        this(2);
    }

    public ExtendedSortedNeighborhoodBlocking(int w) {
        super(w);
        
        LOGGER = Logger.getLogger(ExtendedSortedNeighborhoodBlocking.class.getName());
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it improves " + super.getMethodName() + " by sliding the window over the sorted list of blocking keys.";
    }

    @Override
    public String getMethodName() {
        return "Extended Sorted Neighborhood Blocking";
    }

    @Override
    protected void parseIndex() {
        final Set<String> blockingKeysSet = invertedIndexD1.keySet();
        final String[] sortedTerms = blockingKeysSet.toArray(new String[blockingKeysSet.size()]);
        Arrays.sort(sortedTerms);

        //slide window over the sorted list of blocking keys
        int upperLimit = sortedTerms.length - windowSize;
        for (int i = 0; i <= upperLimit; i++) {
            final TIntSet entityIds = new TIntHashSet();
            for (int j = 0; j < windowSize; j++) {
                entityIds.addAll(invertedIndexD1.get(sortedTerms[i + j]));
            }

            if (1 < entityIds.size()) {
                blocks.add(new UnilateralBlock(entityIds.toArray()));
            }
        }
    }

    @Override
    protected void parseIndices() {
        final Set<String> blockingKeysSet = new HashSet<>();
        blockingKeysSet.addAll(invertedIndexD1.keySet());
        blockingKeysSet.addAll(invertedIndexD2.keySet());
        final String[] sortedTerms = blockingKeysSet.toArray(new String[blockingKeysSet.size()]);
        Arrays.sort(sortedTerms);

        //slide window over the sorted list of blocking keys
        int upperLimit = sortedTerms.length - windowSize;
        for (int i = 0; i <= upperLimit; i++) {
            final TIntSet entityIds1 = new TIntHashSet();
            final TIntSet entityIds2 = new TIntHashSet();
            for (int j = 0; j < windowSize; j++) {
                final TIntList d1Entities = invertedIndexD1.get(sortedTerms[i + j]);
                if (d1Entities != null) {
                    entityIds1.addAll(d1Entities);
                }

                final TIntList d2Entities = invertedIndexD2.get(sortedTerms[i + j]);
                if (d2Entities != null) {
                    entityIds2.addAll(d2Entities);
                }
            }

            if (!entityIds1.isEmpty() && !entityIds2.isEmpty()) {
                blocks.add(new BilateralBlock(entityIds1.toArray(), entityIds2.toArray()));
            }
        }
    }
}
