package edu.yu.oats.oatsdb.dbms.v0;


import edu.yu.oats.oatsdb.dbms.DBMS;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class Log4J2AsyncLogger {
    //private static Logger logger = LogManager.getLogger();
    private static Logger logger = LogManager.getLogger(Log4J2AsyncLogger.class);
    public static void performSomeTask(){
        logger.debug("This is a debug message.");
        logger.info("This is an info message.");
        logger.warn("This is a warn message.");
        logger.error("This is an error message.");
        logger.fatal("This is a fatal message.");
    }

    public static void performSomeTask2(){
        logger.debug("\tThis is a debug message.");
        logger.info("\tThis is an info message.");
        logger.warn("\tThis is a warn message.");
        logger.error("\tThis is an error message.");
        logger.fatal("\tThis is a fatal message.");
    }
    public static void main(String[] args)
    {
        performSomeTask();
        performSomeTask2();
        System.out.println(Long.class);
        logger.info(new String[]{"Rabbi H", "Rabbi M"});
        logger.info(new String[]{"Rabbi H", "Rabbi M"});
    }
}