package net.opentsdb.tsd;
/**
 * the timer job for aggregate metric datapoint
 * Created by wf 201809.
 */

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import org.apache.curator.framework.CuratorFramework;
import org.json.JSONObject;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import net.opentsdb.core.TSDB;
public class MetricQuartz {
	private TSDB tsdb;
	private CuratorFramework client;
	public MetricQuartz(final TSDB tsdb, final CuratorFramework client) {
		this.tsdb = tsdb;
		this.client = client;
    }
	public Scheduler execute() throws SchedulerException {
		Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        try {
            

            //jobdataMap store class object
            JobDataMap jobMap = new JobDataMap();
            jobMap.put("tsdb", tsdb);
            jobMap.put("zkClient", client);
            
            //secondly job
            Trigger secondTrigger = newTrigger().withIdentity("secondTrigger", "group1")
                .startNow()
                .withSchedule(simpleSchedule()
                		.withIntervalInSeconds(30)
                		.repeatForever())
                .build();

            JobDetail secondJob = newJob(MetricSecondlyAggregation.class)
                .withIdentity("secondJob", "group1")
                .usingJobData(jobMap)
                .build();
            JobKey secondJobKey = new JobKey("secondJob", "group1");
            
            //minutely job
            Trigger minuteTrigger = newTrigger().withIdentity("minuteTrigger", "group1")
                    .startNow()
                    .withSchedule(simpleSchedule()
                    		.withIntervalInMinutes(1)
                    		.repeatForever())
                    .build();

            JobDetail minuteJob = newJob(MetricMinutelyAggregation.class)
                    .withIdentity("minuteJob", "group1")
                    .usingJobData(jobMap)
                    .build();
            JobKey minuteJobKey = new JobKey("minuteJob", "group1");
            
            //hourly job
            Trigger hourTrigger = newTrigger().withIdentity("hourTrigger", "group1")
                    .startNow()
                    .withSchedule(simpleSchedule()
                    		.withIntervalInHours(1)
                    		.repeatForever())
                    .build();

            JobDetail hourJob = newJob(MetricHourlyAggregation.class)
                    .withIdentity("hourJob", "group1")
                    .usingJobData("name", "quartz")
                    .usingJobData(jobMap)
                    .build();
            JobKey hourJobKey = new JobKey("hourJob", "group1");
            
            if (null == scheduler.getJobDetail(secondJobKey)) {
            	scheduler.scheduleJob(secondJob, secondTrigger);
            }
            if (null == scheduler.getJobDetail(minuteJobKey)) {
            	scheduler.scheduleJob(minuteJob, minuteTrigger);
            }
            if (null == scheduler.getJobDetail(hourJobKey)) {
            	scheduler.scheduleJob(hourJob, hourTrigger);
            }           
//            scheduler.start();
//            scheduler.isStarted();
//            Thread.sleep(60000);
//            scheduler.shutdown(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scheduler;
    }
}
