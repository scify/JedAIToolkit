/*
* Copyright [2016] [George Papadakis (gpapadis@yahoo.gr)]
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author gap2
 */
public class StandardBlocking extends AbstractBlockBuilding {

    public StandardBlocking() {
        super();
    }
    
    @Override
    protected Set<String> getBlockingKeys(String attributeValue) {
        return new HashSet<>(Arrays.asList(getTokens(attributeValue)));
    }
    
    protected String[] getTokens (String attributeValue) {
        return attributeValue.split("[\\W_]");
    }

    @Override
    public String getMethodInfo() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getMethodParameters() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}