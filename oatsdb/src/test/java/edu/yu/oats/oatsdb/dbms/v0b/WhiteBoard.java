package edu.yu.oats.oatsdb.dbms.v0b;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Use this to record all that's going on in the multithreaded tests
 */
public enum WhiteBoard
{
    Instance;
    private static Logger logger = LogManager.getLogger();
    int counter = 0;//keep track of the order actions come in

    Map<Thread, List<String>> notes = new ConcurrentHashMap<>();

    void setUp_AddThreadToNotes(Thread thread)
    {
        List<String> temp = Collections.synchronizedList(new ArrayList<>());
        notes.put(thread, temp);
    }

    void resetNotes()
    {
        notes = new ConcurrentHashMap<>();
        counter = 0;
    }

    void addAction(Thread thread, String action)
    {
        List<String> threadList = notes.get(thread);
        synchronized (notes) {threadList.add((counter++) + "," + action);}
    }

    void printNotes()
    {
        for (Thread t : notes.keySet()) {
            String debug = "Starting with thread =" + t.getName();
            List<String> actions = notes.get(t);
            debug += "\n-->" + actions.toString();
            logger.debug(debug);
        }
    }
    boolean assertContains(Thread t, String s)
    {
        boolean contains = false;
        List<String> actions = notes.get(t);
        for(int i = 0; i < actions.size(); i++){
            String temp = actions.get(i);
            if(temp.contains(s)) return true;
        }
        return contains;
    }
}
