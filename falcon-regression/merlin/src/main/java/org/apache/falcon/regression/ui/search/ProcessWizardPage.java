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

package org.apache.falcon.regression.ui.search;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.ExecutionType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.ui.Select;
import org.testng.Assert;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

/** Page object of the Process creation page. */
public class ProcessWizardPage extends EntityWizardPage {

    private static final Logger LOGGER = Logger.getLogger(ProcessWizardPage.class);

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "entityForm")
    })
    private WebElement processBox;

    @FindBy(xpath = "//form[@name='processForm']/div[1]")
    private WebElement summaryBox;

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "entityForm"),
        @FindBy(xpath = "//button[contains(@ng-click,'goNext(processForm.$invalid)')]")
    })
    private WebElement nextButton;

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "entityForm"),
        @FindBy(className = "prevBtn")
    })
    private WebElement previousButton;

    @FindBy(xpath = "//a[contains(.,'Cancel')]")
    private WebElement cancelButton;

    @FindBy(xpath = "//fieldset[@id='fieldWrapper']")
    private WebElement formBox;

    public ProcessWizardPage(WebDriver driver) {
        super(driver);
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(processBox, "Process box");
    }

    /**
     * Completes step 1 and clicks next.
     */
    public void goToPropertiesStep(ProcessMerlin process) {
        setProcessGeneralInfo(process);
        clickNext();

    }

    public void goToClustersStep(ProcessMerlin process) {
        goToPropertiesStep(process);

        setProcessPropertiesInfo(process);
        clickNext();
    }

    public void clickNext() {
        nextButton.click();
    }

    public void clickPrevious(){
        previousButton.click();
    }

    public void clickCancel(){
        cancelButton.click();
    }

    /*----- Step1 General info ----*/

    private WebElement getName() {
        return driver.findElement(By.id("entityNameField"));
    }

    public void setName(String name) {
        final WebElement nameElement = getName();
        nameElement.clear();
        for (String s : name.split("")) {
            nameElement.sendKeys(s);
        }
    }
    private WebElement getTagsSection() {
        return driver.findElement(By.id("tagsSection"));
    }

    private WebElement getAddTagButton() {
        return driver.findElement(By.className("formViewContainer"))
            .findElement(By.xpath("//button[contains(.,'add tag')]"));
    }

    private List<WebElement> getDeleteTagButtons() {
        return getTagsSection().findElements(By.xpath("//button[contains(.,'delete')]"));
    }

    private List<WebElement> getTagTextFields() {
        return getTagsSection().findElements(By.tagName("input"));
    }

    public void deleteTags() {
        //delete all tags
        final List<WebElement> deleteTagButtons = getDeleteTagButtons();
        for (int index=deleteTagButtons.size(); index>1; index--) {
            deleteTagButtons.get(index-1).click();
        }
        for (WebElement textField : getTagTextFields()) {
            textField.clear();
        }
    }

    private WebElement getTagKey(int index) {
        return processBox.findElements(By.xpath("//input[@ng-model='tag.key']")).get(index);
    }
    private WebElement getTagValue(int index) {
        return processBox.findElements(By.xpath("//input[@ng-model='tag.value']")).get(index);
    }

    public void setTagKey(int index, String tagKey){
        getTagKey(index).sendKeys(tagKey);
    }
    public void setTagValue(int index, String tagValue){
        getTagValue(index).sendKeys(tagValue);
    }

    public void setTags(String tagsStr){
        if (StringUtils.isEmpty(tagsStr)){
            return;
        }
        String[] tags = tagsStr.split(",");
        for (int i = 0; i < tags.length; i++){
            String[] keyValue = tags[i].split("=");
            setTagKey(i, keyValue[0]);
            setTagValue(i, keyValue[1]);
            if (tags.length > i + 1){
                getAddTagButton().click();
            }
        }
    }

    public String getTagKeyText(int index){
        return getTagKey(index).getAttribute("value");
    }

    public String getTagValueText(int index){
        return getTagValue(index).getAttribute("value");
    }

    public boolean isPigRadioSelected(){
        return getPigRadio().isSelected();
    }

    public String getEngineVersionText(){
        return getEngineVersion().getFirstSelectedOption().getAttribute("value");
    }

    private WebElement getWfName() {
        return driver.findElement(By.id("workflowNameField"));
    }

    private WebElement getOozieRadio() {
        return driver.findElement(By.id("oozieEngineRadio"));
    }

    private WebElement getPigRadio() {
        return driver.findElement(By.id("pigEngineRadio"));
    }

    private WebElement getHiveRadio() {
        return driver.findElement(By.id("hiveEngineRadio"));
    }

    private Select getEngineVersion() {
        return new Select(driver.findElement(By.id("engineVersionField")));
    }

    private WebElement getPath() {
        return driver.findElement(By.id("pathField"));
    }

    public void setWorkflow(Workflow processWf) {
        final WebElement wfName = getWfName();
        wfName.clear();
        wfName.sendKeys(processWf.getName());
        switch (processWf.getEngine()) {
        case OOZIE:
            getOozieRadio().click();
            break;
        case PIG:
            getPigRadio().click();
            break;
        case HIVE:
            getHiveRadio().click();
            break;
        default:
            Assert.fail("Unexpected workflow engine: " + processWf.getEngine());
        }
        final String version = processWf.getVersion();
        // The getVersion() method returns '1.0' if its null, hence the hack below
        if (StringUtils.isNotEmpty(version) && !version.equals("1.0")) {
            getEngineVersion().selectByVisibleText(version);
        }
        final WebElement path = getPath();
        path.clear();
        path.sendKeys(processWf.getPath());
    }

    private WebElement getAclOwner() {
        return driver.findElement(By.name("aclOwnerInput"));
    }

    private WebElement getAclGroup() {
        return driver.findElement(By.name("aclGroupInput"));
    }

    private WebElement getAclPerm() {
        return driver.findElement(By.name("aclPermissionsInput"));
    }

    public void setAcl(ACL acl) {
        final WebElement aclOwner = getAclOwner();
        aclOwner.clear();
        aclOwner.sendKeys(acl.getOwner());
        final WebElement aclGroup = getAclGroup();
        aclGroup.clear();
        aclGroup.sendKeys(acl.getGroup());
        setPermissions(acl.getPermission());
    }

    public void setProcessGeneralInfo(ProcessMerlin process) {
        setName(process.getName());
        final String tags = StringUtils.trimToEmpty(process.getTags());
        setTags(tags);
        setWorkflow(process.getWorkflow());
        setAcl(process.getACL());
    }

    public void isFrequencyQuantityDisplayed(boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getFrequencyQuantity(), "Frequency Quantity");
        }else {
            try{
                getFrequencyQuantity();
                Assert.fail("Frequency Quantity found");
            } catch (Exception ex){
                LOGGER.info("Frequency Quantity not found");
            }
        }
    }

    public void isValidityStartDateDisplayed(boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getStartDate(), "Cluster Validity Start Date");
        }else {
            try{
                getStartDate();
                Assert.fail("Cluster Validity Start Date found");
            } catch (Exception ex){
                LOGGER.info("Cluster Validity Start Date not found");
            }
        }
    }

    public void isAddInputButtonDisplayed(boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getAddInputButton(), "Add Input button.");
        }else {
            try{
                getAddInputButton();
                Assert.fail("Add Input Button found");
            } catch (Exception ex){
                LOGGER.info("Add Input Button not found");
            }
        }
    }

    public void isSaveButtonDisplayed(boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getSaveProcessButton(), "Save Button");
        }else {
            try{
                getSaveProcessButton();
                Assert.fail("Save Process Button found");
            } catch (Exception ex){
                LOGGER.info("Save Process Button not found");
            }
        }
    }

    private WebElement getSaveProcessButton(){
        return formBox.findElement(By.xpath("//button[contains(.,'SAVE')]"));
    }

    public void isTagsDisplayed(int index, boolean isDisplayed){
        if (isDisplayed){
            UIAssert.assertDisplayed(getTagKey(index), "Tag Key Index - " + index);
            UIAssert.assertDisplayed(getTagValue(index), "Tag Value Index - " + index);
        }else{
            try{
                getTagKey(index);
                Assert.fail("Tag Key Index - " + index + " found");
            } catch (Exception ex){
                LOGGER.info("Tag Key Index - " + index + " not found");
            }
            try{
                getTagValue(index);
                Assert.fail("Tag Key Value - " + index + " found");
            } catch (Exception ex){
                LOGGER.info("Tag Key Value - " + index + " not found");
            }
        }
    }

    /*----- Step2 Properties ----*/

    private Select getTimezone() {
        return new Select(formBox.findElement(By.xpath("//select[contains(@class, 'TZSelect')]")));
    }

    public void setTimezone(TimeZone timezone) {
        if (timezone == null) {
            return;
        }
        String timeZone = timezone.getID();
        getTimezone().selectByValue(timeZone);
    }

    private WebElement getFrequencyQuantity() {
        return processBox.findElement(By.xpath("//input[@ng-model='process.frequency.quantity']"));
    }
    private Select getFrequencyUnit() {
        return new Select(processBox.findElement(By.xpath(
            "//select[@ng-model='process.frequency.unit']")));
    }

    public String getFrequencyQuantityText(){
        return getFrequencyQuantity().getAttribute("value");
    }

    public String getMaxParallelInstancesText(){
        return getMaxParallelInstances().getFirstSelectedOption().getAttribute("value");
    }

    public String getTimezoneText(){
        return getTimezone().getFirstSelectedOption().getAttribute("value");
    }

    public String getOrderText(){
        return getOrder().getFirstSelectedOption().getAttribute("value");
    }

    public void setFrequencyQuantity(String frequencyQuantity){
        getFrequencyQuantity().clear();
        getFrequencyQuantity().sendKeys(frequencyQuantity);
    }
    public void setFrequencyUnit(String frequencyUnit){
        getFrequencyUnit().selectByVisibleText(frequencyUnit);
    }

    public List<String> getTimezoneValues(){
        return getDropdownValues(getTimezone());
    }

    public List<String> getFrequencyUnitValues(){
        return getDropdownValues(getFrequencyUnit());
    }

    public List<String> getMaxParallelInstancesValues(){
        return getDropdownValues(getMaxParallelInstances());
    }

    public List<String> getOrderValues(){
        return getDropdownValues(getOrder());
    }

    public List<String> getRetryPolicyValues(){
        return getDropdownValues(getRetryPolicy());
    }

    public List<String> getRetryDelayUnitValues(){
        return getDropdownValues(getRetryDelayUnit());
    }

    private Select getMaxParallelInstances(){
        return new Select(formBox.findElement(By.xpath("//select[@ng-model='process.parallel']")));
    }

    public void setMaxParallelInstances(int quantity) {
        getMaxParallelInstances().selectByValue(String.valueOf(quantity));
    }

    private Select getOrder(){
        return new Select(formBox.findElement(By.xpath("//select[@ng-model='process.order']")));
    }

    public void setOrder(ExecutionType order) {
        getOrder().selectByValue(order.value());
    }

    private Select getRetryPolicy(){
        return new Select(formBox.findElement(By.xpath("//select[@ng-model='process.retry.policy']")));
    }

    private Select getRetryDelayUnit(){
        return new Select(formBox.findElement(By.xpath("//select[@ng-model='process.retry.delay.unit']")));
    }

    private WebElement getAttempts(){
        return formBox.findElement(By.id("attemptsField"));
    }

    private WebElement getDelayQuantity(){
        return formBox.findElement(By.id("delayQuantity"));
    }

    public void setRetry(Retry retry) {
        getRetryPolicy().selectByValue(retry.getPolicy().value());
        getAttempts().clear();
        getAttempts().sendKeys(String.valueOf(retry.getAttempts()));
        getDelayQuantity().clear();
        getDelayQuantity().sendKeys(retry.getDelay().getFrequency());
        getRetryDelayUnit().selectByValue(retry.getDelay().getTimeUnit().name());
    }

    /**
     * Enter process info on Page 2 of processSetup Wizard.
     */
    public void setProcessPropertiesInfo(ProcessMerlin process) {
        setTimezone(process.getTimezone());
        setFrequencyQuantity(process.getFrequency().getFrequency());
        setFrequencyUnit(process.getFrequency().getTimeUnit().toString());
        setMaxParallelInstances(process.getParallel());
        setOrder(process.getOrder());
        setRetry(process.getRetry());
    }

    /*-----Step3 Clusters-------*/

    public WebElement getStartDate() {
        List<WebElement> inputs = driver.findElements(
            By.xpath("//input[contains(@ng-model, 'cluster.validity.start.date')]"));
        return inputs.get(inputs.size() - 1);
    }

    public WebElement getEndDate() {
        List<WebElement> inputs = formBox.findElements(
            By.xpath("//input[contains(@ng-model, 'cluster.validity.end.date')]"));
        return inputs.get(inputs.size() - 1);
    }

    public String getValidityEnd() {
        return String.format("%s %s:%s", getEndDate().getAttribute("value"), getEndHours().getAttribute("value"),
            getEndMinutes().getAttribute("value"));
    }

    public WebElement getStartHours() {
        List<WebElement> inputs = formBox.findElements(By.xpath("//input[contains(@ng-model, 'hours')]"));
        return inputs.get(inputs.size() - 2);
    }

    public WebElement getEndHours() {
        List<WebElement> inputs = formBox.findElements(By.xpath("//input[contains(@ng-model, 'hours')]"));
        return inputs.get(inputs.size() - 1);
    }

    public WebElement getStartMinutes() {
        List<WebElement> inputs = formBox.findElements(By.xpath("//input[contains(@ng-model, 'minutes')]"));
        return inputs.get(inputs.size() - 2);
    }

    public WebElement getEndMinutes() {
        List<WebElement> inputs = formBox.findElements(By.xpath("//input[contains(@ng-model, 'minutes')]"));
        return inputs.get(inputs.size() - 1);
    }

    public WebElement getStartMeredian() {
        List<WebElement> buttons = formBox.findElements(By.xpath("//td[@ng-show='showMeridian']/button"));
        return buttons.get(buttons.size() - 2);
    }

    public WebElement getEndMeredian() {
        List<WebElement> buttons = formBox.findElements(By.xpath("//td[@ng-show='showMeridian']/button"));
        return buttons.get(buttons.size() - 1);
    }

    /**
     * Retrieves the last cluster select.
     */
    public Select getClusterSelect() {
        List<WebElement> selects = formBox.findElements(By.xpath("//select[contains(@ng-model, 'cluster.name')]"));
        return new Select(selects.get(selects.size() - 1));
    }

    public void clickAddClusterButton() {
        int initialSize = getWizardClusterCount();
        formBox.findElement(By.xpath("//button[contains(., 'add cluster')]")).click();
        int finalSize = getWizardClusterCount();
        Assert.assertEquals(finalSize - initialSize, 1, "New cluster block should been added.");
    }

    /**
     * Removes last cluster on the form.
     */
    public void deleteLastCluster() {
        int initialSize = getWizardClusterCount();
        List<WebElement> buttons = formBox.findElements(By.xpath("//button[contains(., 'delete')]"));
        Assert.assertTrue(buttons.size() > 0,
            "Delete button should be present. There should be at least 2 cluster blocks");
        buttons.get(buttons.size() - 1).click();
        int finalSize = getWizardClusterCount();
        Assert.assertEquals(initialSize - finalSize, 1, "One cluster block should been removed.");
    }

    /**
     * Sets multiple clusters in process.
     */
    public void setClusters(Clusters clusters) {
        for (int i = 0; i < clusters.getClusters().size(); i++) {
            if (i > 0) {
                clickAddClusterButton();
            }
            setCluster(clusters.getClusters().get(i));
        }
    }

    /**
     * Fills the last cluster on the form.
     */
    public void setCluster(Cluster cluster) {
        selectCluster(cluster.getName());
        setClusterValidity(cluster);
    }

    /**
     * Populates cluster form with values from process.Cluster object.
     * @param cluster process process.Cluster object
     */
    public void setClusterValidity(Cluster cluster) {
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy-hh-mm-a");
        String start = format.format(cluster.getValidity().getStart());
        String [] parts = start.split("-");
        getStartDate().clear();
        sendKeysSlowly(getStartDate(), parts[0]);
        getStartHours().clear();
        sendKeysSlowly(getStartHours(), parts[1]);
        getStartMinutes().clear();
        sendKeysSlowly(getStartMinutes(), parts[2]);
        String meredian = getStartMeredian().getText();
        if (!meredian.equals(parts[3])) {
            getStartMeredian().click();
        }
        String end = format.format(cluster.getValidity().getEnd());
        parts = end.split("-");
        getEndDate().clear();
        sendKeysSlowly(getEndDate(), parts[0]);
        getEndHours().clear();
        sendKeysSlowly(getEndHours(), parts[1]);
        getEndMinutes().clear();
        sendKeysSlowly(getEndMinutes(), parts[2]);
        meredian = getEndMeredian().getText();
        if (!meredian.equals(parts[3])) {
            getEndMeredian().click();
        }
    }

    public void selectCluster(String clusterName) {
        getClusterSelect().selectByValue(clusterName);
    }

    public String getClusterName(int indx) {
        List<WebElement> blocks = formBox.findElements(By.xpath("//div[contains(@class, 'processCluster')]"));
        return new Select(blocks.get(indx).findElement(By.tagName("select")))
            .getFirstSelectedOption().getText();
    }

    public int getWizardClusterCount() {
        return formBox.findElements(By.xpath("//div[contains(@class, 'processCluster')]")).size();
    }

    public void setProcessClustersInfo(ProcessMerlin process) {
        for (int i = 0; i < process.getClusters().getClusters().size(); i++) {
            if (i >= 1) {
                clickAddClusterButton();
            }
            setCluster(process.getClusters().getClusters().get(i));
        }
    }

    public List<String> getClustersFromDropDown() {
        return getDropdownValues(getClusterSelect());
    }

    public void clickOnValidityStart() {
        getStartDate().click();
        List<WebElement> calendars = formBox.findElements(By.xpath("//ul[@ng-model='date']"));
        waitForAngularToFinish();
        Assert.assertTrue(calendars.get(calendars.size() - 2).isDisplayed(), "Calendar should pop up.");
    }

    public void clickOnValidityEnd() {
        getEndDate().click();
        List<WebElement> calendars = formBox.findElements(By.xpath("//ul[@ng-model='date']"));
        waitForAngularToFinish();
        Assert.assertTrue(calendars.get(calendars.size() - 1).isDisplayed(), "Calendar should pop up.");
    }

    /* Step 4 - Inputs & Outputs*/

    private WebElement getAddInputButton() {
        return formBox.findElement(By.xpath("//button[contains(., 'add input')]"));
    }

    private WebElement getAddOutputButton() {
        return formBox.findElement(By.xpath("//button[contains(., 'add output')]"));
    }

    private WebElement getDeleteInputButton() {
        return formBox.findElement(By.xpath("//button[contains(., 'delete')]"));
    }

    private WebElement getInputName(int index) {
        return formBox.findElements(By.xpath("//input[@ng-model='input.name']")).get(index);
    }

    private Select getInputFeed(int index) {
        return new Select(formBox.findElements(By.xpath("//select[@ng-model='input.feed']")).get(index));
    }

    private WebElement getInputStart(int index) {
        return formBox.findElements(By.xpath("//input[@ng-model='input.start']")).get(index);
    }

    private WebElement getInputEnd(int index) {
        return formBox.findElements(By.xpath("//input[@ng-model='input.end']")).get(index);
    }

    public void setInputInfo(Inputs inputs){
        for (int i = 0; i < inputs.getInputs().size(); i++) {
            clickAddInput();
            sendKeysSlowly(getInputName(i), inputs.getInputs().get(i).getName());
            getInputFeed(i).selectByVisibleText(inputs.getInputs().get(i).getFeed());
            sendKeysSlowly(getInputStart(i), inputs.getInputs().get(i).getStart());
            sendKeysSlowly(getInputEnd(i), inputs.getInputs().get(i).getEnd());
            //clickCheckBoxSecurely(getOptionalCheckbox(), inputs.getInputs().get(i).isOptional());
        }
    }

    private WebElement getOptionalCheckbox() {
        return formBox.findElement(By.xpath("//input[@ng-model='input.optional']"));
    }

    public boolean isOptionalSelected() {
        return getOptionalCheckbox().isSelected();
    }

    public void clickAddInput(){
        waitForAngularToFinish();
        getAddInputButton().click();
    }

    public void clickAddOutput(){
        waitForAngularToFinish();
        getAddOutputButton().click();
    }

    public void clickDeleteInput(){
        getDeleteInputButton().click();
    }

    private WebElement getDeleteOutputButton() {
        return formBox.findElement(By.xpath("//button[contains(., 'delete')]"));
    }

    private WebElement getOutputName(int index) {
        return formBox.findElements(By.xpath("//input[@ng-model='output.name']")).get(index);
    }

    private Select getOutputFeed(int index) {
        return new Select(formBox.findElements(By.xpath("//select[@ng-model='output.feed']")).get(index));
    }

    private WebElement getOutputInstance(int index) {
        return formBox.findElements(By.xpath("//input[@ng-model='output.outputInstance']")).get(index);
    }

    public void clickDeleteOutput(){
        getDeleteOutputButton().click();
    }

    public void setOutputInfo(Outputs outputs){
        for (int i = 0; i < outputs.getOutputs().size(); i++) {
            clickAddOutput();
            sendKeysSlowly(getOutputName(i), outputs.getOutputs().get(i).getName());
            getOutputFeed(i).selectByVisibleText(outputs.getOutputs().get(i).getFeed());
            getOutputInstance(i).clear();
            sendKeysSlowly(getOutputInstance(i), outputs.getOutputs().get(i).getInstance());
        }
    }

    public void setInputOutputInfo(ProcessMerlin process){
        setInputInfo(process.getInputs());
        setOutputInfo(process.getOutputs());
    }

    public List<String> getInputValues(int index){
        return getDropdownValues(getInputFeed(index));
    }

    public List<String> getOutputValues(int index){
        return getDropdownValues(getOutputFeed(index));
    }

    public String getInputNameText(int index){
        return getInputName(index).getAttribute("value");
    }

    public String getInputFeedText(int index){
        return getInputFeed(index).getFirstSelectedOption().getAttribute("value");
    }

    public String getInputStartText(int index){
        return getInputStart(index).getAttribute("value");
    }

    public String getInputEndText(int index){
        return getInputEnd(index).getAttribute("value");
    }

    public String getOutputNameText(int index){
        return getOutputName(index).getAttribute("value");
    }

    public String getOutputFeedText(int index){
        return getOutputFeed(index).getFirstSelectedOption().getAttribute("value");
    }

    public String getOutputInstanceText(int index){
        return getOutputInstance(index).getAttribute("value");
    }

    public void isInputNameDisplayed(int index, boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getInputName(index), "Input Name " + index);
        }else {
            try{
                getInputName(index);
                Assert.fail("Input Name " + index + " found");
            } catch (Exception ex){
                LOGGER.info("Input Name " + index + " not found");
            }
        }
    }

    public void isOutputNameDisplayed(int index, boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getOutputName(index), "Output Name " + index);
        }else {
            try{
                getOutputName(index);
                Assert.fail("Output Name " + index + " found");
            } catch (Exception ex){
                LOGGER.info("Output Name " + index + " not found");
            }
        }
    }


    /* Step 5 - Summary */

    public void clickSave(){
        getSaveProcessButton().click();
        waitForAlert();
    }

    @Override
    public ProcessMerlin getEntityFromXMLPreview() {
        return new ProcessMerlin(getXMLPreview());
    }

    @Override
    public WebElement getEditXMLButton() {
        return driver.findElement(By.id("editXmlButton"));
    }

    @Override
    public WebElement getRevertXMLButton() {
        return driver.findElement(By.id("revertXMLBtn"));
    }

    /**
     * Method gets text from summary box and parses it to ProcessMerlin object.
     * @param draft empty ProcessMerlin object
     */
    public ProcessMerlin getProcessFromSummaryBox(ProcessMerlin draft) {
        draft.setName(summaryBox.findElement(By.xpath("//span[@data-qe-id='processName']")).getText());

        String tags = "";
        List<WebElement> tagElements = summaryBox.findElements(By.xpath("//span[@data-qe-id='processTags']"));
        WebElement tagElement;
        for (int index=0; index<tagElements.size(); index++) {
            tagElement = tagElements.get(index);
            String tag = tagElement.getText();
            if (!tag.contains("No tags")) {
                tag = tag.replace(" ", "");
                tags = tags + (tags.isEmpty() || (index == tagElements.size()-1) ? tag : "," + tag);
            }
        }
        if (!tags.isEmpty()) {
            draft.setTags(tags);
        }

        Workflow workflow = new Workflow();
        String workflowName = summaryBox.findElement(By.xpath("//label[@data-qe-id='processWorkflowName']")).getText();
        if (StringUtils.isNotBlank(workflowName)) {
            workflow.setName(workflowName);
        }
        String workflowVersion = summaryBox.findElement(
                By.xpath("//label[@data-qe-id='processWorkflowVersion']")).getText();
        if (StringUtils.isNotBlank(workflowVersion)) {
            workflow.setVersion(workflowVersion);
        }
        String workflowEngine = summaryBox.findElement(
                By.xpath("//label[@data-qe-id='processWorkflowEngine']")).getText();
        if (StringUtils.isNotBlank(workflowEngine)) {
            workflow.setEngine(EngineType.fromValue(workflowEngine));
        }
        workflow.setPath(summaryBox.findElement(By.xpath("//label[@data-qe-id='processWorkflowPath']")).getText());
        draft.setWorkflow(workflow);

        draft.setTimezone(TimeZone.getTimeZone(
            summaryBox.findElement(By.xpath("//label[@data-qe-id='processTimezone']")).getText()));
        draft.setFrequency(new Frequency(
            summaryBox.findElement(By.xpath("//span[@data-qe-id='processFrequencyQuantity']")).getText(),
            Frequency.TimeUnit.valueOf(
                summaryBox.findElement(By.xpath("//span[@data-qe-id='processFrequencyUnit']")).getText())));
        draft.setParallel(Integer.parseInt(
            summaryBox.findElement(By.xpath("//label[@data-qe-id='processParallel']")).getText()));
        draft.setOrder(ExecutionType.fromValue(
            summaryBox.findElement(By.xpath("//label[@data-qe-id='processOrder']")).getText()));

        draft.setACL(summaryBox.findElement(By.xpath("//span[@data-qe-id='processACLOwner']")).getText(),
            summaryBox.findElement(By.xpath("//span[@data-qe-id='processACLGroup']")).getText(),
            summaryBox.findElement(By.xpath("//span[@data-qe-id='processACLPermission']")).getText());

        Retry retry = new Retry();
        retry.setPolicy(PolicyType.fromValue(
            summaryBox.findElement(By.xpath("//label[@data-qe-id='processRetryPolicy']")).getText()));
        retry.setAttempts(Integer.parseInt(
            summaryBox.findElement(By.xpath("//label[@data-qe-id='processRetryAttempts']")).getText()));
        retry.setDelay(new Frequency(
            summaryBox.findElement(By.xpath("//span[@data-qe-id='processRetryDelayQuantity']")).getText(),
            Frequency.TimeUnit.valueOf(
                summaryBox.findElement(By.xpath("//span[@data-qe-id='processRetryDelayUnit']")).getText())));
        draft.setRetry(retry);

        //get clusters
        List<WebElement> clusterElements = summaryBox.findElements(By.xpath("//div[@data-qe-id='processClusters']"));
        for (int index = 0; index < clusterElements.size(); index++) {
            Cluster cluster = new Cluster();
            cluster.setName(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processClusterName']")).get(index).getText());
            Validity validity = new Validity();
            String start = summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processClusterStart']")).get(index).getText();
            String end = summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processClusterEnd']")).get(index).getText();
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy'-'MM'-'dd' 'HH':'mm'");
            validity.setStart(formatter.parseDateTime(start.replaceAll("\"", "")).toDate());
            validity.setEnd(formatter.parseDateTime(end.replaceAll("\"", "")).toDate());
            cluster.setValidity(validity);
            draft.getClusters().getClusters().add(cluster);
        }

        //get inputs
        List<WebElement> processInputs = summaryBox.findElements(By.xpath("//div[@data-qe-id='processInputs']"));
        for (int index = 0; index < processInputs.size(); index++) {
            Input input = new Input();
            //get input name
            input.setName(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processInputName']")).get(index).getText());
            //get input feed
            input.setFeed(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processInputFeed']")).get(index).getText());
            //get input start
            input.setStart(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processInstanceStart']")).get(index).getText());
            //get input end
            input.setEnd(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processInstanceEnd']")).get(index).getText());
            draft.getInputs().getInputs().add(input);
        }
        //get outputs
        List<WebElement> processOutputs = summaryBox.findElements(By.xpath("//div[@data-qe-id='processOutputs']"));
        for (int index = 0; index < processOutputs.size(); index++) {
            Output output = new Output();
            output.setName(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processOutputName']")).get(index).getText());
            //get feed
            output.setFeed(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processOutputFeed']")).get(index).getText());
            output.setInstance(summaryBox.findElements(
                By.xpath("//label[@data-qe-id='processOutputInstance']")).get(index).getText());
            draft.getOutputs().getOutputs().add(output);
        }
        //check compulsory process properties
        Assert.assertNotNull(draft.getACL(), "ACL is empty (null).");
        return draft;
    }

    /**
     * Retrieves property from source text.
     */
    private String getProperty(String block, String start, String end, int propertyIndex) {
        int s = start != null ? block.indexOf(start) : 0;
        s = s == -1 ? 0 : s;
        int e = end != null ? block.indexOf(end) : block.length() - 1;
        e = e == -1 ? block.length() : e;
        String subBlock = block.substring(s, e).trim();
        String [] parts = subBlock.trim().split(":");
        return parts.length - 1 >= propertyIndex ? parts[propertyIndex].trim() : null;
    }
}
