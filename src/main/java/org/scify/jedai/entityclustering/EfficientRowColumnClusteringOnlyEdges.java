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
package org.scify.jedai.entityclustering;

import com.esotericsoftware.minlog.Log;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import org.apache.jena.ext.com.google.common.collect.ComparisonChain;
import org.scify.jedai.datamodel.Comparison;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Manos
 */
public class EfficientRowColumnClusteringOnlyEdges extends AbstractCcerEntityClustering {
    private static final long serialVersionUID = 3375022141607586773L;

    //protected float[][] matrix; // inverted similarity matrix (cost matrix)
    protected TIntFloatMap[] matrixHM; // inverted similarity matrix (cost matrix)

    protected int[] selectedRow, selectedColumn, columnsFromSelectedRow;

    protected float costRowScan, costColumnScan;

    protected boolean[] isRowCovered, isColumnCovered;

    protected float[] edgesWeightRows;
    protected float[] edgesWeightColumns;


    public EfficientRowColumnClusteringOnlyEdges() {
        this(0.5f);
    }

    public EfficientRowColumnClusteringOnlyEdges(float simTh) {
        super(simTh);
    }

    public void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    private int columnWithMin(int rowNumber) {
        int pos = -1;
        float min = Float.MAX_VALUE;
        if (matrixHM[rowNumber] != null) {
            for (int col : matrixHM[rowNumber].keys()) {
                if (isColumnCovered[col]) {
                    continue;
                }
                //if they are not connected do not consider matching
                if (!matrixHM[rowNumber].containsKey(col)) {
                    continue;
                }

                if (matrixHM[rowNumber].get(col) < min) {
                    pos = col;
                    min = matrixHM[rowNumber].get(col);
                }
            }
        }
        return pos;
    }

    private void getColumnAssignment() {
        List<Integer> columnsSortedWithTotalWeight= new ArrayList<>();
        for (int col = 0; col< (noOfEntities-datasetLimit); col++) {
            columnsSortedWithTotalWeight.add(col);
        }
        columnsSortedWithTotalWeight.sort((r2, r1) -> {
            return ComparisonChain.start().compare(edgesWeightColumns[r1], edgesWeightColumns[r2]).result();
        });
        //for (int I:columnsSortedWithTotalWeight) System.out.println(I+" col "+ edgesWeightColumns[I]);
        costColumnScan = 0;
        for (int I:columnsSortedWithTotalWeight) {
            int col=I;
            selectedRow[col] = rowWithMin(col);
            if (selectedRow[col]==-1) continue;
            columnsFromSelectedRow[selectedRow[col]] = col;
            isRowCovered[selectedRow[col]] = true;
            costColumnScan += matrixHM[selectedRow[col]].get(col);


        }
    }

    @Override
    public EquivalenceCluster[] getDuplicates(SimilarityPairs simPairs) {
        Log.info("Input comparisons\t:\t" + simPairs.getNoOfComparisons());

        matchedIds.clear();
        if (simPairs.getNoOfComparisons() == 0) {
            return new EquivalenceCluster[0];
        }

        initializeData(simPairs);
        if (!isCleanCleanER) {
            return null; //the method is only applicable to Clean-Clean ER
        }

        final Iterator<Comparison> iterator = simPairs.getPairIterator();

        final TIntFloatMap[] simMatrix = new TIntFloatHashMap[datasetLimit];
        edgesWeightRows = new float[datasetLimit];
        edgesWeightColumns = new float[noOfEntities-datasetLimit];
        while (iterator.hasNext()) {
            final Comparison comparison = iterator.next();
            if (threshold < comparison.getUtilityMeasure()) {
                int entityId1 = comparison.getEntityId1();
                if (simMatrix[entityId1] == null) {
                    simMatrix[entityId1] = new TIntFloatHashMap();
                }
                simMatrix[entityId1].put(comparison.getEntityId2(), 1.0f - comparison.getUtilityMeasure());
            }
        }

        /*float[][] simMatrix = new float[datasetLimit][noOfEntities - datasetLimit];
        edgesWeightRows = new float[datasetLimit];
        edgesWeightColumns = new float[noOfEntities-datasetLimit];
        //System.out.println("noofentities "+noOfEntities);
        while (iterator.hasNext()) {
            final Comparison comparison = iterator.next();
            if (threshold < comparison.getUtilityMeasure()) {
                edgesWeightRows[comparison.getEntityId1()] += comparison.getUtilityMeasure();
                edgesWeightColumns[comparison.getEntityId2()] += comparison.getUtilityMeasure();
                simMatrix[comparison.getEntityId1()][comparison.getEntityId2()] = comparison.getUtilityMeasure();
            }
        }*/

        init(simMatrix);

        int[] solutionProxy = getSolution();

        for (int i = 0; i < solutionProxy.length; i++) {
            int e1 = i;
            int e2 = solutionProxy[i];
            if ((e2==-1)) {
                continue;
            }
            if (simMatrix[e1]==null) {
                continue;
            }
            if (!simMatrix[e1].containsKey(e2)) {
                continue;
            }
            if (1.0f-simMatrix[e1].get(e2) < threshold) {
                continue;
            }
            e2 += datasetLimit;
            //skip already matched entities (unique mapping contraint for clean-clean ER)
            if (matchedIds.contains(e1) || matchedIds.contains(e2)) {
                System.err.println("id already in the graph");
            }

            similarityGraph.addEdge(e1, e2);
            matchedIds.add(e1);
            matchedIds.add(e2);
        }

        return getConnectedComponents();
    }

    @Override
    public String getMethodInfo() {
        return getMethodName() + ": it create a cluster after approximately solving the assignment problem. ";
    }

    @Override
    public String getMethodName() {
        return "Row-Column Proxy Clustering Considering Only Existing Edges not using 2d float matrix";
    }
    //Do not consider matches below threshold

    private void getRowAssignment() {
        List<Integer> rowsSortedWithTotalWeight= new ArrayList<>();
        for (int row = 0; row < datasetLimit; row++) {
            rowsSortedWithTotalWeight.add(row);
        }
        rowsSortedWithTotalWeight.sort((r2, r1) -> {
            return ComparisonChain.start().compare(edgesWeightRows[r1], edgesWeightRows[r2]).result();
        });
        costRowScan = 0;
        for (int I:rowsSortedWithTotalWeight)
        {
            int row=I;
            selectedColumn[row] = columnWithMin(row);
            if (selectedColumn[row]==-1) continue;

            isColumnCovered[selectedColumn[row]] = true;
            costRowScan += matrixHM[row].get(selectedColumn[row]);
        }
    }

    private int[] getSolution() {
        getRowAssignment();
        getColumnAssignment();
        if (costRowScan < costColumnScan) {
            return selectedColumn;
        } else {
            return columnsFromSelectedRow;
        }
    }

    private void init(TIntFloatMap[] matrix) {
        this.matrixHM = matrix;

        this.selectedColumn = new int[datasetLimit];
        this.isColumnCovered = new boolean[noOfEntities-datasetLimit];

        this.selectedRow = new int[noOfEntities-datasetLimit];
        this.columnsFromSelectedRow = new int[datasetLimit];
        this.isRowCovered = new boolean[datasetLimit];
        /*System.out.println("numRows "+matrixHM.length);
        for (int i=0;i<matrixHM.length;i++)
        {
            System.out.println(i);
            for (int col : matrixHM[i].keys()) {
                System.out.println(col+" "+matrixHM[i].get(col));
            }
            }
        System.out.println();*/
    }

    private int rowWithMin(int columnNumber) {
        int pos = -1;
        float min = Float.MAX_VALUE;
        for (int row = 0; row < datasetLimit; row++) {
            if (isRowCovered[row]) {
                continue;
            }
            //if they are not connected do not consider matching
            if (matrixHM[row] != null) {
                if (!matrixHM[row].containsKey(columnNumber)) {
                    continue;
                }
                if (matrixHM[row].get(columnNumber) < min) {
                    pos = row;
                    min = matrixHM[row].get(columnNumber);
                }
            }
        }
        return pos;
    }
}