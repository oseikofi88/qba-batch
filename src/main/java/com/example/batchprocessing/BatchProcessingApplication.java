package com.example.batchprocessing;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class BatchProcessingApplication implements CommandLineRunner {
	private final FileWatcherService fileWatcherService;

	public BatchProcessingApplication(FileWatcherService fileWatcherService) {
		this.fileWatcherService = fileWatcherService;
	}

	public static void main(String[] args) {
		SpringApplication.run(BatchProcessingApplication.class, args);
	}

	@Override
	public void run(String... args) {
		fileWatcherService.startWatching();
	}
}