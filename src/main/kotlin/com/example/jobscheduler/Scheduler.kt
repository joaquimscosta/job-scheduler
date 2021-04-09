package com.example.jobscheduler

import mu.KotlinLogging
import org.quartz.*
import org.quartz.SimpleScheduleBuilder.simpleSchedule
import org.quartz.impl.matchers.GroupMatcher
import org.springframework.scheduling.quartz.QuartzJobBean
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.*
import java.io.Serializable
import java.time.Instant
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

const val JOB_DATA_KEY: String = "timerInfo"

@Component
class HelloWorldJob :
    QuartzJobBean() {
    val logger = KotlinLogging.logger {}

    override fun executeInternal(context: JobExecutionContext) {
        logger.debug { "Hello World @ ${Instant.now()}" }
        val job = context.mergedJobDataMap[JOB_DATA_KEY] as? TimerInfo
        job?.let {
            logger.debug { it }
        }
    }
}

class FireCountListener(
    private val scheduleService: ScheduleService
) : TriggerListener {
    val logger = KotlinLogging.logger {}

    override fun getName(): String = FireCountListener::class.java.simpleName

    override fun triggerComplete(
        trigger: Trigger,
        context: JobExecutionContext,
        triggerInstructionCode: Trigger.CompletedExecutionInstruction?
    ) {
    }

    override fun triggerFired(
        trigger: Trigger,
        context: JobExecutionContext
    ) {
        val jobName = trigger.jobKey.name
        val job = context.mergedJobDataMap[JOB_DATA_KEY] as? TimerInfo
        if (job == null) {
            logger.debug { "Job not found" }
            return
        }
        if (job.totalFireCount == 0) {
            return
        }

        job.totalFireCount -= 1
        scheduleService.updateJob(jobName, job)
    }

    override fun triggerMisfired(trigger: Trigger) {
    }

    override fun vetoJobExecution(
        trigger: Trigger,
        context: JobExecutionContext
    ): Boolean {
        // if true the job execute will not be invoked
        return false
    }
}

@RestController
@RequestMapping("/api/schedule")
class SchedulerController(private val scheduleService: ScheduleService) {
    @PostMapping
    fun createJob() {
        val job = TimerInfo(
            totalFireCount = 5,
            repeatIntervalMs = 5000,
            initialOffsetMs = 0,
            runForEver = false
        )
        scheduleService.schedule(HelloWorldJob::class.java, job)
    }

    @GetMapping
    fun findAllJobs(): List<TimerInfo> {
        return scheduleService.findAllJobs()
    }

    @GetMapping("{jobName}")
    fun findJob(@PathVariable jobName: String): TimerInfo? {
        return scheduleService.findById(jobName)
    }

    @DeleteMapping("{jobName}")
    fun deleteJob(@PathVariable jobName: String): Boolean {
        return scheduleService.deleteJob(jobName)
    }
}

@Service
class ScheduleService(private val scheduler: Scheduler) {
    val logger = KotlinLogging.logger {}


    @PostConstruct
    fun init() {
        try {
            scheduler.start()
        } catch (e: SchedulerException) {
            logger.error(e) { "Error starting scheduler" }
        }
    }

    @PreDestroy
    fun destroy() {
        try {
            scheduler.shutdown()
        } catch (e: SchedulerException) {
            logger.error(e) { "Error closing scheduler " }
        }
    }

    fun findAllJobs(): List<TimerInfo> {
        return scheduler.getJobKeys(GroupMatcher.anyGroup())
            .mapNotNull {
                val job = scheduler.getJobDetail(it)
                job.jobDataMap[JOB_DATA_KEY] as? TimerInfo
            }
    }

    fun findById(jobName: String): TimerInfo? {
        val job = scheduler.getJobDetail(JobKey(jobName))
        if (job == null) {
            logger.debug { "Job not found $jobName" }
            return null
        }
        return job.jobDataMap[JOB_DATA_KEY] as? TimerInfo
    }


    fun schedule(
        jobClass: Class<out Job>,
        timerInfo: TimerInfo
    ) {
        val job = TimerUtils.buildJobDetail(jobClass, timerInfo)
        val trigger = TimerUtils.buildTrigger(timerInfo)
        try {
            scheduler.scheduleJob(job, trigger)
        } catch (e: SchedulerException) {
            logger.error(e) { "Failed to scheduling job" }
        }
    }

    fun deleteJob(jobName: String): Boolean {
        return scheduler.deleteJob(JobKey(jobName))
    }

    fun updateJob(
        jobName: String,
        timerInfo: TimerInfo
    ) {
        val job = scheduler.getJobDetail(JobKey(jobName))
        if (job == null) {
            logger.debug { "Job not found $jobName" }
            return
        }
        job.jobDataMap[JOB_DATA_KEY] = timerInfo
        scheduler.addJob(job, true)
    }
}

data class TimerInfo(
    var totalFireCount: Int,
    var repeatIntervalMs: Long,
    var initialOffsetMs: Long = 0,
    var runForEver: Boolean = false,
    var callbackData: String = "",
    var jobName: UUID = UUID.randomUUID()
) : Serializable

object TimerUtils {
    fun buildJobDetail(
        jobClass: Class<out Job>,
        timerInfo: TimerInfo
    ): JobDetail {
        val key = JobKey(timerInfo.jobName.toString())
        val data = JobDataMap().apply {
            this[JOB_DATA_KEY] = timerInfo
        }
        return JobBuilder
            .newJob(jobClass)
            .withIdentity(key)
            .usingJobData(data)
            .build()
    }

    fun buildTrigger(timerInfo: TimerInfo): Trigger {
        val scheduleBuilder =
            simpleSchedule().withIntervalInMilliseconds(timerInfo.repeatIntervalMs)
        if (timerInfo.runForEver) {
            scheduleBuilder.repeatForever()
        } else {
            scheduleBuilder.withRepeatCount(timerInfo.totalFireCount - 1)
        }
        val startAt = Date.from(Instant.now().plusMillis(timerInfo.initialOffsetMs))
        return TriggerBuilder
            .newTrigger()
            .withSchedule(scheduleBuilder)
            .startAt(startAt)
            .build()
    }
}





