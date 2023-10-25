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
package org.scify.jedai.generalexamples;

import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.IdDuplicates;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 */
public class CleanCleanErDatasetWriteout {

    /**
     * Make sure your Java VM has enough Heap Ram. cleanDBPedia2 only ran with 20GB
     * heap on my system.
     * I invoked java with -Xmx20g.
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();

        //String mainFolder = "changeme";
        String[] entitiesFiles = { mainFolder + "cleanDBPedia1", mainFolder + "cleanDBPedia2" };
        String groundTruthFile = mainFolder + "newDBPediaMatches";
        for (String entitiesFile : entitiesFiles) {
            String fileName = entitiesFile + "out";
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            System.out.println("Current dataset\t:\t" + entitiesFile);
            IEntityReader eReader = new EntitySerializationReader(entitiesFile);
            List<EntityProfile> profiles = eReader.getEntityProfiles();
            System.out.println("Input Entity Profiles\t:\t" + profiles.size());
            final Set<String> distinctAttributes = new HashSet<>();
            double nameValuePairs = 0;
            int nProc = 0;
            for (EntityProfile profile : profiles) {
                if (nProc % 10000 == 0) {
                    System.out.println(nProc + "/" + profiles.size());
                }
                writer.write(dump(profile, nProc));
                nameValuePairs += profile.getProfileSize();
                for (Attribute a : profile.getAttributes()) {
                    distinctAttributes.add(a.getName());
                }
                nProc += 1;
            }
            writer.close();
            System.out.println("Data has been written to " + fileName);
            System.out.println("Distinct attributes\t:\t" + distinctAttributes.size());
            System.out.println("Total Name-Value Pairs\t:\t" + nameValuePairs);
            System.out.println("Average Name-Value Pairs\t:\t" + nameValuePairs / profiles.size());
        }
        writeOutGroundtruth(groundTruthFile);
    }

    public static void writeOutGroundtruth(String fileName) throws IOException {
        System.out.println("Groundtruth dataset\t:\t" + fileName);
        String outFileName = fileName + "out";
        final String sep = " , ";
        BufferedWriter writer = new BufferedWriter(new FileWriter(outFileName));
        IGroundTruthReader gtReader = new GtSerializationReader(fileName);
        final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(
                gtReader.getDuplicatePairs(null));
        Set<IdDuplicates> duplicates = duplicatePropagation.getDuplicates();
        for (IdDuplicates dup : duplicates) {
            writer.write(dup.getEntityId1() + sep + dup.getEntityId2() + "\n");
        }
        System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());
        writer.close();
    }

    /**
     * Escaping tactic: Replace ',' in attribute by ',,'.
     * Use 'space,space' as separator. This cannot appear in attributes, entityUrl
     * after escaping ','.
     * 
     * @param id of profile
     * @return String representation of profile for storage in file
     */
    public static String dump(EntityProfile profile, int id) {
        StringBuilder sb = new StringBuilder();
        final String sep = " , ";
        sb.append(id).append(sep);
        sb.append(normalizeForDump(profile.getEntityUrl())).append(sep);
        sb.append(profile.getProfileSize()).append(sep);
        profile.getAttributes().forEach(attribute -> {
            sb.append(normalizeForDump(attribute.getName())).append(sep);
            sb.append(normalizeForDump(attribute.getValue())).append(sep);
        });
        sb.append("\n");
        return sb.toString();
    }

    private static String normalizeForDump(String s) {
        return org.apache.commons.lang3.StringEscapeUtils
                .unescapeJava(s.replaceAll("\\R", " ").replace(",", ",,")).replaceAll("\\R", " ");
    }
}
