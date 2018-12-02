package com.techprudent.batch;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class SpringBatchDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchDemoApplication.class, args);
	}
}


class PeopleItemProcessor implements ItemProcessor<People, People> {

    private static final Logger log = LoggerFactory.getLogger(PeopleItemProcessor.class);

    @Override
    public People process(final People people) throws Exception {
        final String segment = people.getSegment().toUpperCase();
        final String city = people.getCity().toUpperCase();

        people.setCity(city);
        people.setSegment(segment);
        
        log.info("Converting (" + people + ") into (" + people + ")");

        return people;
    }

}

@Configuration
@EnableBatchProcessing
class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private JobCompletionNotificationListener listener;
    
    @Bean
    public FlatFileItemReader<People> reader() {
        return new FlatFileItemReaderBuilder<People>()
            .name("PeopleItemReader")
            .resource(new ClassPathResource("Sample - Superstore.csv"))
            .delimited()
				.names(new String[] { "rowId", "orderId", "orderDate", "shipDate", "customerId",
						"customerName", "segment", "city", "state", "sales", "quantity", "discount", "profit"
            })
            .fieldSetMapper(new BeanWrapperFieldSetMapper<People>() {{
                setTargetType(People.class);
            }})
            .build();
    }

    @Bean
    public PeopleItemProcessor processor() {
        return new PeopleItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<People> writer() {
        return new JdbcBatchItemWriterBuilder<People>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO tableau.people (row_id, order_id,"
            		+ "order_date,ship_date,customer_id,customer_name,segment,city,state,sales,"
            		+ "quantity,discount,profit) "
            		+ "VALUES (:rowId, :orderId, :orderDate, :shipDate, :customerId, "
            		+ ":customerName, :segment, :city, :state, :sales, :quantity, :discount, :profit)")
            .dataSource(dataSource)
            .build();
    }

    @Bean
    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(step1())
            .end()
            .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
            .<People, People> chunk(100)
            .reader(reader())
            .processor(processor())
            .writer(writer())
            .build();
    }
}

@Component
class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");

			int queryForObject = jdbcTemplate.queryForObject("SELECT count(*) FROM tableau.people", Integer.class);
			System.out.println("Inserted count: " + queryForObject);
		}
	}
}
