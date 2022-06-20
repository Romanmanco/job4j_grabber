package ru.job4j.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

/**
 * Начало работы происходит с создания класса управляющего всеми работами.
 * В объект Scheduler мы будем добавлять задачи, которые хотим выполнять периодически.
 */
public class AlertRabbit {

    public static void main(String[] args) {
        Properties properties = readFile();
        try (Connection connection = connect(properties)) {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            JobDataMap data = new JobDataMap();
            data.put("store", connection);
            JobDetail job = newJob(Rabbit.class)
                    .usingJobData(data)
                    .build();
            SimpleScheduleBuilder times = simpleSchedule()
                    .withIntervalInSeconds(
                            Integer.parseInt(properties.getProperty("rabbit.interval")))
                    .repeatForever();
            Trigger trigger = newTrigger()
                    .startNow()
                    .withSchedule(times)
                    .build();
            scheduler.scheduleJob(job, trigger);
            Thread.sleep(10000);
            scheduler.shutdown();
        } catch (SchedulerException | InterruptedException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static class Rabbit implements Job {

        public Rabbit() {
            System.out.println(hashCode());
        }

        @Override
        public void execute(JobExecutionContext context) {
            System.out.println("Rabbit runs here ...");
            Connection connection =
                    (Connection) context.getJobDetail().getJobDataMap().get("store");
//            try (PreparedStatement ps = connection.prepareStatement(
//                    "insert into rabbit(created_date) values(?);")) {
//                ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
//                ps.executeUpdate();
//            }
            try (PreparedStatement statement =
                         connection.prepareStatement("insert into rabbit(created_date) values (?)",
                                 Statement.RETURN_GENERATED_KEYS)) {
                statement.setLong(1, System.currentTimeMillis());
                statement.execute();
            } catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }
    }

    public static Properties readFile() {
        Properties properties = new Properties();
        try (InputStream in = AlertRabbit.class
                .getClassLoader()
                .getResourceAsStream("rabbit.properties")) {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static Connection connect(Properties properties) throws SQLException, ClassNotFoundException {
        Class.forName(properties.getProperty("driver"));
        return DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("username"),
                properties.getProperty("password")
        );
    }
}