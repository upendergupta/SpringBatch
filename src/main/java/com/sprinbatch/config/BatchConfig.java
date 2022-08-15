package com.sprinbatch.config;

import com.sprinbatch.entity.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig 
{
  @Autowired
  private DataSource datasource;
  @Autowired
  private JobBuilderFactory jobBuilderFactory;
   
  @Autowired
  private StepBuilderFactory stepBuilderFactory;
 
  @Bean
  public Job readCSVFilesJob() {
    return jobBuilderFactory
        .get("readCSVFilesJob")
        .incrementer(new RunIdIncrementer())
        .flow(step1()).end()
        .build();
  }
 
  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1").<Employee, Employee>chunk(10)
        .reader(reader())
        .processor(processor())
        .writer(writer())
        .build();
  }

  @Bean
  public FlatFileItemReader<Employee> reader()
  {
    //Create reader instance
    FlatFileItemReader<Employee> reader = new FlatFileItemReader<Employee>();
     
    //Set input file location
    reader.setResource(new ClassPathResource("employees.csv"));
     
    //Set number of lines to skips. Use it if file has header rows.
    reader.setLinesToSkip(1);   
     
    //Configure how each line will be parsed and mapped to different values
    reader.setLineMapper(new DefaultLineMapper<Employee>() {
      {
        //3 columns in each row
        setLineTokenizer(new DelimitedLineTokenizer() {
          {
            setNames(new String[] { "EMP ID","Name Prefix", "First Name" ,"Last Name"});
            setIncludedFields(new int[]{0,1,2,4});
          }
        });
        //Set values in Employee class
        setFieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {
          {
            setTargetType(Employee.class);
          }
        });
      }
    });
    return reader;
  }
   
  @Bean
  public JdbcBatchItemWriter<Employee> writer()
  {
    JdbcBatchItemWriter<Employee> writer=new JdbcBatchItemWriter<>();
    writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
    writer.setSql("insert into employee(userId,namePrefix,firstName,lastName) values (:userId,:namePrefix,:firstName,:lastName)");
    writer.setDataSource(this.datasource);
    return writer;
  }

  @Bean
  public EmployeeItemProcessor processor(){
    return new EmployeeItemProcessor();
  }
}