/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.Entities;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class RecipeMerlin {
    private static final Logger LOGGER = Logger.getLogger(RecipeMerlin.class);
    private static final String WORKFLOW_PATH_KEY = "falcon.recipe.workflow.path";
    private static final String RECIPE_NAME_KEY = "falcon.recipe.name";
    private static final String WRITE_DIR =
        Config.getProperty("recipe.location", "/tmp/falcon-recipe");

    private String template;
    private AbstractFileConfiguration properties;
    private String workflow;
    private FalconCLI.RecipeOperation recipeOperation;

    private RecipeMerlin() {
    }

    public String getName() {
        return properties.getString(RECIPE_NAME_KEY);
    }

    public void setUniqueName(String prefix) {
        properties.setProperty(RECIPE_NAME_KEY, prefix + UUID.randomUUID().toString().split("-")[0]);
    }

    public RecipeMerlin withSourceDb(final String srcDatabase) {
        properties.setProperty("sourceDatabase", srcDatabase);
        return this;
    }

    public RecipeMerlin withSourceTable(final String tgtTable) {
        properties.setProperty("sourceTable", tgtTable);
        return this;
    }

    public RecipeMerlin withSourceCluster(ClusterMerlin srcCluster) {
        properties.setProperty("sourceCluster", srcCluster.getName());
        properties.setProperty("sourceMetastoreUri", srcCluster.getProperty("hive.metastore.uris"));
        properties.setProperty("sourceHiveServer2Uri", srcCluster.getProperty("hive.server2.uri"));
        //properties.setProperty("sourceServicePrincipal",
        //    srcCluster.getProperty("hive.metastore.kerberos.principal"));
        properties.setProperty("sourceStagingPath", srcCluster.getLocation("STAGING"));
        properties.setProperty("sourceNN", srcCluster.getInterfaceEndpoint(Interfacetype.WRITE));
        properties.setProperty("sourceRM", srcCluster.getInterfaceEndpoint(Interfacetype.EXECUTE));
        return this;
    }

    public RecipeMerlin withTargetCluster(ClusterMerlin tgtCluster) {
        properties.setProperty("targetCluster", tgtCluster.getName());
        properties.setProperty("targetMetastoreUri", tgtCluster.getProperty("hive.metastore.uris"));
        properties.setProperty("targetHiveServer2Uri", tgtCluster.getProperty("hive.server2.uri"));
        //properties.setProperty("targetServicePrincipal",
        //    tgtCluster.getProperty("hive.metastore.kerberos.principal"));
        properties.setProperty("targetStagingPath", tgtCluster.getLocation("STAGING"));
        properties.setProperty("targetNN", tgtCluster.getInterfaceEndpoint(Interfacetype.WRITE));
        properties.setProperty("targetRM", tgtCluster.getInterfaceEndpoint(Interfacetype.EXECUTE));
        return this;
    }

    public RecipeMerlin withRecipeCluster(ClusterMerlin recipeCluster) {
        properties.setProperty("falcon.recipe.cluster.name", recipeCluster.getName());
        properties.setProperty("falcon.recipe.cluster.hdfs.writeEndPoint",
            recipeCluster.getInterfaceEndpoint(Interfacetype.WRITE));
        return this;
    }

    public RecipeMerlin withValidity(final String start, final String end) {
        properties.setProperty("falcon.recipe.cluster.validity.start", start);
        properties.setProperty("falcon.recipe.cluster.validity.end", end);
        return this;
    }

    public RecipeMerlin withFrequency(final Frequency frequency) {
        properties.setProperty("falcon.recipe.process.frequency", frequency);
        return this;
    }

    /**
     * Read recipe from a given directory. Expecting that recipe will follow these conventions.
     * <br> 1. properties file will have .properties extension
     * <br> 2. template file will have end with -template.xml
     * <br> 3. workflow file will have end with -workflow.xml
     * @param readPath the location from where recipe will be read
     * @param recipeOperation operation of this recipe
     */
    public static RecipeMerlin readFromDir(final String readPath,
                                           FalconCLI.RecipeOperation recipeOperation) {
        Assert.assertTrue(StringUtils.isNotEmpty(readPath), "readPath for recipe can't be empty");
        Assert.assertNotNull(recipeOperation, "readPath for recipe can't be empty");
        RecipeMerlin instance = new RecipeMerlin();
        instance.recipeOperation = recipeOperation;
        LOGGER.info("Loading recipe from directory: " + readPath);
        File directory = null;
        try {
            directory = new File(RecipeMerlin.class.getResource("/" + readPath).toURI());
        } catch (URISyntaxException e) {
            Assert.fail("could not find dir: " + readPath);
        }
        final Collection<File> propertiesFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*\\.properties"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(propertiesFiles.size(), 1,
            "Expecting only one property file at: " + readPath +" found: " + propertiesFiles);
        try {
            instance.properties =
                new PropertiesConfiguration(propertiesFiles.iterator().next());
        } catch (ConfigurationException e) {
            Assert.fail("Couldn't read recipe's properties file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        instance.properties.setFileName(null); //prevent accidental overwrite of template
        //removing defaults - specific test need to supplied this
        instance.properties.clearProperty("sourceDatabase");
        instance.properties.clearProperty("sourceTable");
        instance.properties.clearProperty("targetDatabase");
        instance.properties.clearProperty("targetTable");

        final Collection<File> templatesFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*-template\\.xml"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(templatesFiles.size(), 1,
            "Expecting only one template file at: " + readPath + " found: " + templatesFiles);
        try {
            instance.template =
                FileUtils.readFileToString(templatesFiles.iterator().next());
        } catch (IOException e) {
            Assert.fail("Couldn't read recipe's template file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }

        final Collection<File> workflowFiles = FileUtils.listFiles(directory,
            new RegexFileFilter(".*-workflow\\.xml"), FalseFileFilter.INSTANCE);
        Assert.assertEquals(workflowFiles.size(), 1,
            "Expecting only one workflow file at: " + readPath + " found: " + workflowFiles);
        try {
            instance.workflow = FileUtils.readFileToString(workflowFiles.iterator().next());
        } catch (IOException e) {
            Assert.fail("Couldn't read recipe's workflow file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        return instance;
    }

    /**
     * Write recipe.
     */
    private void write() {
        final String templateFileLocation = OSUtil.getPath(WRITE_DIR, getName() + "-template.xml");
        try {
            Assert.assertNotNull(templateFileLocation,
                "Write location for template file is unexpectedly null.");
            FileUtils.writeStringToFile(new File(templateFileLocation), template);
        } catch (IOException e) {
            Assert.fail("Couldn't write recipe's template file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }

        final String workflowFileLocation = OSUtil.getPath(WRITE_DIR, getName() + "-workflow.xml");
        try {
            Assert.assertNotNull(workflowFileLocation,
                "Write location for workflow file is unexpectedly null.");
            FileUtils.writeStringToFile(new File(workflowFileLocation), workflow);
        } catch (IOException e) {
            Assert.fail("Couldn't write recipe's workflow file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
        properties.setProperty(WORKFLOW_PATH_KEY, workflowFileLocation);
        properties.setProperty("falcon.recipe.workflow.name", getName() + "-workflow");

        final String propFileLocation = OSUtil.getPath(WRITE_DIR, getName() + ".properties");
        try {
            Assert.assertNotNull(propFileLocation,
                "Write location for properties file is unexpectedly null.");
            properties.save(new File(propFileLocation));
        } catch (ConfigurationException e) {
            Assert.fail("Couldn't write recipe's process file because of exception: "
                + ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * Get submission command.
     */
    public List<String> getSubmissionCommand() {
        write();
        final List<String> cmd = new ArrayList<String>();
        Collections.addAll(cmd, "recipe", "-name", getName(),
            "-operation", recipeOperation.toString());
        return cmd;
    }

}
