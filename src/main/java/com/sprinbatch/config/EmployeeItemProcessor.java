package com.sprinbatch.config;

import com.sprinbatch.entity.Employee;
import org.springframework.batch.item.ItemProcessor;

public class EmployeeItemProcessor implements ItemProcessor<Employee,Employee> {
    @Override
    public Employee process(Employee item) throws Exception {

        return item;
    }
}
