package com.example.batchprocessing;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Bean
	@StepScope
	public FlatFileItemReader<Discount> reader(@Value("#{jobParameters['filePaths']}") String filePath) {
		return new FlatFileItemReaderBuilder<Discount>()
				.name("personItemReader")
				.resource(new FileSystemResource(filePath))
				.delimited()
				.names("name", "percentage")
				.targetType(Discount.class)
				.build();
	}

	@Bean
	public DiscountItemProcessor processor() {
		return new DiscountItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Discount> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Discount>()
				.sql("INSERT INTO discount (id, name, percentage, created_by) VALUES (uuid_generate_v4(), :name, :percentage, 'admin')")
				.dataSource(dataSource)
				.beanMapped()
				.build();
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
		return new JobBuilder("importUserJob", jobRepository)
				.listener(listener)
				.start(step1)
				.build();
	}

	@Bean
	public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
			FlatFileItemReader<Discount> reader, DiscountItemProcessor processor,
			JdbcBatchItemWriter<Discount> writer) {
		return new StepBuilder("step1", jobRepository)
				.<Discount, Discount>chunk(3, transactionManager)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}
	// end::jobstep[]
}
