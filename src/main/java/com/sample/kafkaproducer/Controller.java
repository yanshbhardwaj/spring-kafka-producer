package com.sample.kafkaproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class Controller {
    Logger logger = LoggerFactory.getLogger(Controller.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public Controller(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Autowired
    KafkaTemplate<String, Employee> kafkaTemplate;

    private static final String TOPIC = "KafkaTopic2";

    @PostMapping("/publish")
    public String publishMessage()
    {
        List<Employee> employeeList = selectAllEmployee();
        for (Employee employee : employeeList) {
            logger.info("Producer is Publishing data to the Topic=" + TOPIC);
            kafkaTemplate.send(TOPIC, employee);
            logger.info("Data successfully published to the Topic.");
        }
        return "Published Successfully";
    }

    public List<Employee> selectAllEmployee() {
        String sql = "select emp_id, first_name from employee";
        List<Employee> query = jdbcTemplate.query(sql, (resultSet, i) -> new Employee(Integer.parseInt(resultSet.getString(1)), resultSet.getString(2)));
        return query;
    }

}
