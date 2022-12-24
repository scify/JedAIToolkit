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

import java.io.File;
import java.util.HashSet;
import org.apache.log4j.BasicConfigurator;
import org.scify.jedai.datamodel.Attribute;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;

import java.util.List;
import java.util.Set;
import org.scify.jedai.datamodel.IdDuplicates;

/**
 *
 * @author G.A.P. II
 */
public class PrintDatasets {

    private static List<EntityProfile> printDataset(String filePath) {
        final IEntityReader eReader = new EntitySerializationReader(filePath);
        List<EntityProfile> profiles = eReader.getEntityProfiles();
        System.out.println("\n\n\n\n\nDataset\t:\t" + filePath);
        System.out.println("Number of Entity Profiles\t:\t" + profiles.size());

        Set<String> attributes = new HashSet<>();
        for (EntityProfile profile : profiles) {
            for (Attribute attribute : profile.getAttributes()) {
                attributes.add(attribute.getName());
            }
        }
        System.out.println("Distinct attributes\t:\t" + attributes);
        
        for (EntityProfile profile : profiles) {
            System.out.println("\nProfile id\t:\t" + profile.getEntityUrl());
            for (Attribute attribute : profile.getAttributes()) {
                System.out.println(attribute.getName() + " : " + attribute.getValue());
            }
        }
        
        return profiles;
    }

    private static void printGroundtruth(List<EntityProfile> profilesD1, List<EntityProfile> profilesD2, String filePath) {
        IGroundTruthReader gtReader = new GtSerializationReader(filePath);
        Set<IdDuplicates> duplicates = gtReader.getDuplicatePairs(null);
        System.out.println("\n\n\n\n\nDataset groundtruth\t:\t" + filePath);
        for (IdDuplicates pair : duplicates) {
            System.out.println("\nPair " + profilesD1.get(pair.getEntityId1()).getEntityUrl() + "\t" + profilesD2.get(pair.getEntityId2()).getEntityUrl());
        }
    }
    
    public static void main(String[] args) {
        BasicConfigurator.configure();

        String[] entitiesFilePath = {"data" + File.separator + "cleanCleanErDatasets" + File.separator + "abtProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "buyProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "amazonProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "gpProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "acmProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpProfiles2",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "scholarProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "imdbProfiles",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dbpediaProfiles"
        };
        String[] groundTruthFilePath = {"data" + File.separator + "cleanCleanErDatasets" + File.separator + "abtBuyIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "amazonGpIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpAcmIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "dblpScholarIdDuplicates",
            "data" + File.separator + "cleanCleanErDatasets" + File.separator + "moviesIdDuplicates"
        };

        for (int i = 0; i < groundTruthFilePath.length; i++) {
            System.out.println("\n\nCurrent dataset\t:\t" + groundTruthFilePath[i]);

            List<EntityProfile> profiles1 = printDataset(entitiesFilePath[i * 2]);
            List<EntityProfile> profiles2 = printDataset(entitiesFilePath[i * 2 + 1]);
            printGroundtruth(profiles1, profiles2, groundTruthFilePath[i]);
        }
    }
}
