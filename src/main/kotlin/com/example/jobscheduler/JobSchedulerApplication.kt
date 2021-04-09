package com.example.jobscheduler

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class JobSchedulerApplication

fun main(args: Array<String>) {
	runApplication<JobSchedulerApplication>(*args)
}
