package org.wso2.mb.integration.common.utils.ui.pages.main;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.testng.Assert;
import org.wso2.mb.integration.common.utils.ui.UIElementMapper;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This page represents ' Dead Letter Channel -> Browse -> Queue Content' page in MB management console.
 */
public class DLCContentPage {
    private static final Log log = LogFactory.getLog(DLCContentPage.class);
    private WebDriver driver;

    /**
     * Retrieve page consists content of DeadLetter Channel
     *
     * @param driver selenium web driver used to run the test
     * @throws IOException if mapper.properties file not found
     */
    public DLCContentPage(WebDriver driver) throws IOException {
        this.driver = driver;
        // Check that we're on the right page.
        if (!driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                 .getElement("mb.dlc.queue.content"))).getText()
                .contains("Queue Content: DeadLetterChannel")) {
            throw new IllegalStateException("This is not the DLC Queue Content page");
        }
    }

    /**
     * Test deleting messages of DeadLetter Channel
     */
    public String deleteFunction() throws IOException {
        String deletingMessageID;

        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.choose.box.xpath"))).click();
        deletingMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                .getElement("mb.dlc.first.message.id"))).getText();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.delete.button"))).click();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.function.confirm"))).click();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.function.success"))).click();
        return deletingMessageID;
    }

    /**
     * Delete all the messages in DeadLetter Channel
     */
    public void deleteAllDLCMessages() {

        String deletingMessageID = null;

        try {
            deletingMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                     .getElement("mb.dlc.first.message.id"))).getText();
            if(StringUtils.isNotBlank(deletingMessageID)) {
                log.info("delete all dlc messages");
                driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                    .getElement("mb.dlc.browse.table.choose.all.box.xpath")))
                                                    .click();
                driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                    .getElement("mb.dlc.browse.table.delete.button"))).click();
                driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                    .getElement("mb.dlc.browse.function.confirm"))).click();
                driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                    .getElement("mb.dlc.browse.function.success"))).click();
            }

        }catch (NoSuchElementException e) {
            log.error("unable to find any valid messages in dead letter channel.");
        }


    }

    /**
     * Test restoring messages of DeadLetter Channel
     */
    public String restoreFunction() throws IOException {
        String restoringMessageID;

        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.choose.box.xpath"))).click();
        restoringMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                 .getElement("mb.dlc.first.message.id"))).getText();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.restore.button"))).click();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.function.confirm"))).click();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.function.success"))).click();

        return restoringMessageID;
    }

    /**
     * Test rerouting messages of DeadLetter Channel
     */
    public String rerouteFunction(String qName) throws IOException {
        String reroutingMessageID;

        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.choose.box.xpath"))).click();
        reroutingMessageID = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                 .getElement("mb.dlc.first.message.id"))).getText();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.reroute.button"))).click();
        //select rerouteTestQueue to reroute message
        WebElement select = driver.findElement(By.xpath(UIElementMapper.getInstance()
                                                                .getElement("mb.dlc.browse.table.reroute.queue.select")));
        List<WebElement> options = select.findElements(By.tagName(UIElementMapper.getInstance()
                                                                          .getElement("mb.dlc.browse.table.reroute.queue.option")));
        for (WebElement option : options) {
            if (option.getText().equals(qName)) {
                option.click();
                break;
            }
        }
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.table.reroute.confirm"))).click();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.function.confirm"))).click();
        driver.findElement(By.xpath(UIElementMapper.getInstance()
                                            .getElement("mb.dlc.browse.function.success"))).click();
        return reroutingMessageID;
    }

}
