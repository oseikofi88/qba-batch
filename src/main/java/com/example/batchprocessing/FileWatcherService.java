package com.example.batchprocessing;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;

@Service
public class FileWatcherService {
    private final JobLauncher jobLauncher;
    private final Job processFileJob;
    private final Path watchPath;
    private WatchService watchService;
    private volatile boolean running = true;
    private final ScheduledExecutorService batchExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Set<String> pendingFiles = new HashSet<>();

    public FileWatcherService(JobLauncher jobLauncher, Job processFileJob,
            @Value("${file.watch.path:/home/richard/Downloads/old-gs-batch-processing-main/src/main/resources/}") String directoryPath) {
        this.jobLauncher = jobLauncher;
        this.processFileJob = processFileJob;
        this.watchPath = Paths.get(directoryPath);
        initializeWatcher();
    }

    private void initializeWatcher() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            watchPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            System.out.println("✅ Watching directory: " + watchPath);
        } catch (IOException e) {
            throw new RuntimeException("❌ Failed to initialize WatchService", e);
        }
    }

    @Async("taskExecutor") // Uses a thread pool, non-blocking
    public void startWatching() {
        while (running) {
            try {
                WatchKey key = watchService.take(); // Efficient blocking
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path newFile = watchPath.resolve((Path) event.context());
                    synchronized (pendingFiles) {
                        pendingFiles.add(newFile.toString());
                    }
                    scheduleBatchProcessing(); // Schedule a batch job
                }
                key.reset();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("🛑 File watching interrupted.");
            } catch (Exception e) {
                System.err.println("❌ Error in FileWatcher: " + e.getMessage());
            }
        }
        System.out.println("🛑 FileWatcherService stopped.");
    }

    private void scheduleBatchProcessing() {
        batchExecutor.schedule(() -> {
            synchronized (pendingFiles) {
                if (!pendingFiles.isEmpty()) {
                    try {
                        startBatchJob(String.join(",", pendingFiles)); // Process all files together
                        pendingFiles.clear();
                    } catch (Exception e) {
                        System.err.println("❌ Error triggering batch job: " + e.getMessage());
                    }
                }
            }
        }, 2, TimeUnit.SECONDS); 
    }

    private void startBatchJob(String filePaths) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePaths", filePaths)
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(processFileJob, jobParameters);
    }

    @PreDestroy
    public void stopWatching() {
        System.out.println("🛑 Stopping FileWatcherService...");
        running = false;
        batchExecutor.shutdown();
        try {
            watchService.close();
        } catch (IOException e) {
            System.err.println("❌ Error closing WatchService: " + e.getMessage());
        }
    }
}
