package org.apache.ivory.entity;

import org.apache.ivory.entity.parser.Frequency;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EntityUtilTest {

    private Date getDate(String date) throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
        return format.parse(date);
    }

    @Test
    public void testGetNextStartTime() throws Exception {
        Date now = getDate("2012-04-03 02:45 UTC");
        Date start = getDate("2012-04-02 03:00 UTC");
        Date newStart = getDate("2012-04-03 03:00 UTC");

        Frequency frequency = Frequency.hours;
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, 1, "UTC", now));
    }

    @Test
    public void testgetNextStartTimeOld() throws Exception {
        Date now = getDate("2012-05-02 02:45 UTC");
        Date start = getDate("2012-02-01 03:00 UTC");
        Date newStart = getDate("2012-05-02 03:00 UTC");

        Frequency frequency = Frequency.days;
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, 7, "UTC", now));
    }

    @Test
    public void testGetNextStartTime2() throws Exception {
        Date now = getDate("2010-05-02 04:45 UTC");
        Date start = getDate("2010-02-01 03:00 UTC");
        Date newStart = getDate("2010-05-03 03:00 UTC");

        Frequency frequency = Frequency.days;
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, 7, "UTC", now));
    }

    @Test
    public void testGetNextStartTime3() throws Exception {
        Date now = getDate("2010-05-02 04:45 UTC");
        Date start = getDate("1980-02-01 03:00 UTC");
        Date newStart = getDate("2010-05-07 03:00 UTC");

        Frequency frequency = Frequency.days;
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, 7, "UTC", now));
    }


    @Test
    public void testGetInstanceSequence() throws Exception {
        Date instance = getDate("2012-05-22 13:40 UTC");
        Date start = getDate("2012-05-14 07:40 UTC");

        Frequency frequency = Frequency.hours;
        Assert.assertEquals(199, EntityUtil.getInstanceSequence(start,
                frequency, 1, "UTC", instance));
    }

    @Test
    public void testGetInstanceSequence1() throws Exception {
        Date instance = getDate("2012-05-22 12:40 UTC");
        Date start = getDate("2012-05-14 07:40 UTC");

        Frequency frequency = Frequency.hours;
        Assert.assertEquals(198, EntityUtil.getInstanceSequence(start,
                frequency, 1, "UTC", instance));
    }

    @Test
    public void testGetInstanceSequence2() throws Exception {
        Date instance = getDate("2012-05-22 12:41 UTC");
        Date start = getDate("2012-05-14 07:40 UTC");

        Frequency frequency = Frequency.hours;
        Assert.assertEquals(199, EntityUtil.getInstanceSequence(start,
                frequency, 1, "UTC", instance));
    }
}
