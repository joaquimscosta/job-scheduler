package com.example.jobscheduler

import io.rsocket.loadbalance.LoadbalanceRSocketClient
import io.rsocket.loadbalance.LoadbalanceTarget
import io.rsocket.util.DefaultPayload
import mu.KotlinLogging
import org.quartz.*
import org.quartz.SimpleScheduleBuilder.simpleSchedule
import org.quartz.impl.matchers.GroupMatcher
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.messaging.rsocket.RSocketRequester
import org.springframework.messaging.rsocket.annotation.ConnectMapping
import org.springframework.scheduling.quartz.QuartzJobBean
import org.springframework.stereotype.Component
import org.springframework.stereotype.Controller
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.io.Serializable
import java.time.Instant
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

const val JOB_DATA_KEY: String = "timerInfo"

@Component
class HelloWorldJob(
    private val rSocketClients: RSocketClients
) :
    QuartzJobBean() {
    val logger = KotlinLogging.logger {}

    override fun executeInternal(context: JobExecutionContext) {
        logger.debug { "Executing job @ ${Instant.now()}" }
        val job = context.mergedJobDataMap[JOB_DATA_KEY] as? TimerInfo
        job?.let { logger.debug { "jobData= $it" } }

        logger.info { "clients size = ${rSocketClients.clientList().size}" }

        rSocketClients.clientList()
            .forEach { requester ->
                val msg = DefaultPayload.create("Hello 123?")
                requester.rsocketClient()
                    .fireAndForget(Mono.just(msg))
                    .subscribe()
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

@Component
class RSocketClients {
    private val clients = mutableListOf<RSocketRequester>()

    fun addClient(rSocketRequester: RSocketRequester): Boolean = clients.add(rSocketRequester)
    fun removeClient(rSocketRequester: RSocketRequester): Boolean = clients.remove(rSocketRequester)

    fun clientList(): List<RSocketRequester> = clients.toList()

    fun loadBalancedClient(): LoadbalanceRSocketClient {

//        LoadbalanceTarget.from("", )

       val rSocketRequester =  clients.first()
        val loadBalanced = LoadbalanceRSocketClient.builder {


        }.build()

        return loadBalanced
    }
}

@Controller
class SchedulerController(
    private val scheduleService: ScheduleService,
    private val rSocketClients: RSocketClients
) {
    private val logger = KotlinLogging.logger { }

    @ConnectMapping("setup")
    fun handle(requester: RSocketRequester) {
        logger.info { "******* entering handle ******" }
        logger.info { "requester= $requester" }
        requester
            .rsocket()
            ?.run {
                onClose()
                    .doFirst {
                        logger.info("Client CONNECTED")
                        rSocketClients.addClient(requester)
                    }
                    .doOnError {
                        logger.warn { "Channel to client CLOSED" }
                    }
                    .doFinally {
                        rSocketClients.removeClient(requester)
                        logger.info("Client DISCONNECTED")
                    }
                    .subscribe()
            }
    }

    @MessageMapping("schedule.create")
    fun createJob(job: TimerInfo): Mono<Date?> {
//        val job = TimerInfo(
//            totalFireCount = 5,
//            repeatIntervalMs = 5000,
//            initialOffsetMs = 0,
//            runForEver = false
//        )
        val time = scheduleService.schedule(HelloWorldJob::class.java, job)
        if (time != null) {
            return Mono.just(time)
        }
        return Mono.empty()
    }

    @MessageMapping("schedule.all")
    fun findAllJobs(): Flux<TimerInfo> {
        return scheduleService.findAllJobs().toFlux()
    }

//    @MessageMapping("schedule.get/{jobName}")
//    fun findJob(@PathVariable jobName: String): TimerInfo? {
//        return scheduleService.findById(jobName)
//    }
//
//    @MessageMapping("schedule.delete/{jobName}")
//    fun deleteJob(@PathVariable jobName: String): Boolean {
//        return scheduleService.deleteJob(jobName)
//    }
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
    ): Date? {
        val job = TimerUtils.buildJobDetail(jobClass, timerInfo)
        val trigger = TimerUtils.buildTrigger(timerInfo)
        return try {
            scheduler.scheduleJob(job, trigger)
        } catch (e: SchedulerException) {
            logger.error(e) { "Failed to scheduling job" }
            null
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
    var startTime: Instant = Instant.now(),
    var repeatIntervalMs: Long = 0,
    var runForEver: Boolean = false,
    var description: String = "",
    var jobId: UUID = UUID.randomUUID(),
    var totalFireCount: Int = 5,
) : Serializable

object TimerUtils {
    fun buildJobDetail(
        jobClass: Class<out Job>,
        timerInfo: TimerInfo
    ): JobDetail {
        val key = JobKey(timerInfo.jobId.toString())
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
        println("timerInfo.totalFireCount: ${timerInfo.totalFireCount}")
        val trigger = TriggerBuilder
            .newTrigger()
            .startAt(Date.from(timerInfo.startTime))
        if (timerInfo.repeatIntervalMs > 0) {
            val scheduleBuilder =
                simpleSchedule()
                    .withIntervalInMilliseconds(timerInfo.repeatIntervalMs)
            if (timerInfo.runForEver) {
                scheduleBuilder.repeatForever()
            } else {
                scheduleBuilder.withRepeatCount(timerInfo.totalFireCount - 1)
            }
            trigger.withSchedule(scheduleBuilder)
        }
        return trigger.build()
    }
}





