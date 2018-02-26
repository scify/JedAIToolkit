/*
* Copyright [2016-2018] [George Papadakis (gpapadis@yahoo.gr)]
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
package org.scify.jedai.utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import org.scify.jedai.datamodel.EquivalenceCluster;
import gnu.trove.iterator.TIntIterator;

/**
 *
 * @author gap2
 */
public class PrintToFile {

    public static void toCSV(List<EquivalenceCluster> entityClusters, String filename) throws FileNotFoundException {
        final PrintWriter pw = new PrintWriter(new File(filename));
        final StringBuilder sb = new StringBuilder();

        int counter = 0;
        for (EquivalenceCluster eqc : entityClusters) {
            if (eqc.getEntityIdsD1().isEmpty()) {
                continue;
            }
            counter++;
            sb.append("\nEquivalence cluster : ").append(counter).append("\n").append("Dataset 1 : ");;
            for (TIntIterator iterator = eqc.getEntityIdsD1().iterator(); iterator.hasNext();) {
                sb.append(iterator.next());
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append('\n');
            if (eqc.getEntityIdsD2().isEmpty()) {
                continue;
            }
            sb.append("Dataset 2 : ");
//            sb.deleteCharAt(sb.length() - 1);
            for (TIntIterator iterator = eqc.getEntityIdsD2().iterator(); iterator.hasNext();) {
                sb.append(iterator.next());
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append('\n');
        }
        pw.write(sb.toString());
        pw.close();
    }
}
