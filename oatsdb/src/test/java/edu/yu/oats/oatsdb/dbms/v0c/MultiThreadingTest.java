package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.NoSuchElementException;

public class MultiThreadingTest
{
    private static Logger logger = LogManager.getLogger();
    boolean printNote = false;
    boolean vLog = false;
    boolean vPrintStackTrace = false;
    private final static boolean vPrintName = true;
    boolean printWhy = true;//if I have to manually close a tx afterwards, unless I say otherwise, I want to log that
    ///////////////
    //   notes   //
    ///////////////
    /*
     * -> If you start 2 threads 5 mill apart they can overlap
     */

    ///////////////
    //  helper   //
    ///////////////

    void addToNotes(Thread t, String s)
    {
        WhiteBoard.Instance.addAction(t, s);
    }

    void setPrintNote(boolean val)
    {
        printNote = val;
    }

    void setvPrintStackTrace(boolean b)
    {
        vPrintStackTrace = b;
    }

    void printNotes()
    {
        WhiteBoard.Instance.printNotes();
    }

    boolean assertContains(Thread t, String s)
    {
        return WhiteBoard.Instance.assertContains(t, s);
    }

    /////////////////
    //  Runables   //
    /////////////////
    Runnable getAResource500Mill = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting Ten milli");
                //waitMilli(10);
                Thread.sleep(500);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");
            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    Runnable getAResourceFifteenMillAndAddResourceSarah = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting fifteen milli");
                Thread.sleep(15);
                addToNotes(Thread.currentThread(), "About to add \"Sarah\"");
                firstLast.put("Sarah", 34);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    Runnable getAResourceFifteenMillAndAddResourceRachel = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting Fifteen milli");
                //waitMilli(10);
                Thread.sleep(15);
                addToNotes(Thread.currentThread(), "About to add \"Rachel\"");
                firstLast.put("Rachel", 42);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    //delete a key that doesn't exist
    Runnable deleteAResourceFiftyMillAndAddResourceRebecca = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, deleteing Howard");
                firstLast.remove("Howard");
                addToNotes(Thread.currentThread(), "Deleted Howard -- waiting Fifteen milli");
                //waitMilli(10);
                Thread.sleep(50);
                addToNotes(Thread.currentThread(), "About to add \"Rebecca\"");
                firstLast.put("Rebecca", 42);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    //delete a key that doesn't exist
    Runnable deleteAResourceFiftyMillAndAddResourceSarit = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, deleteing Howard");
                firstLast.remove("Howard");
                addToNotes(Thread.currentThread(), "Deleted Howard -- waiting Fifteen milli");
                //waitMilli(10);
                Thread.sleep(50);
                addToNotes(Thread.currentThread(), "About to add \"Sarit\"");
                firstLast.put("Sarit", 77);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };

    Runnable putAResource30MillAndAddResourceHanna22andHoward55 = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, about to add Howard");
                firstLast.put("Howard", 55);
                addToNotes(Thread.currentThread(), "Adding Howard -- waiting thirty milli");
                Thread.sleep(30);
                addToNotes(Thread.currentThread(), "About to add \"Hanna\"");
                firstLast.put("Hanna", 22);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    Runnable putAResource30MillAndAddResourceMiley33andHoward56 = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, about to add Howard");
                firstLast.put("Howard", 56);
                addToNotes(Thread.currentThread(), "Adding Howard -- waiting thirty milli");
                Thread.sleep(30);
                addToNotes(Thread.currentThread(), "About to add \"Miley\"");
                firstLast.put("Miley", 33);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };

    Runnable getAResourceFiftyMillAndAddResourceHaily66 = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting fifty milli");
                Thread.sleep(50);
                addToNotes(Thread.currentThread(), "About to add \"Haily\"");
                firstLast.put("Haily", 66);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    Runnable getAResourceFiftyMillAndAddResourceAshly78 = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting Fifty milli");
                //waitMilli(10);
                Thread.sleep(50);
                addToNotes(Thread.currentThread(), "About to add \"Ashly\"");
                firstLast.put("Ashly", 78);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };

    //specific for tx1CommitsThenTx2Commits3
    Runnable removeAResourceFifteenMillRemoveEmma = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting Fifteen milli");
                Thread.sleep(15);
                Map<String, String> namePlace = DBMSImpl.Instance.getMap("tx1CommitsThenTx2Commits3", String.class, String .class);
                addToNotes(Thread.currentThread(), "About to remove \"Emma\"");
                namePlace.remove("Emma");
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    //specific for tx1CommitsThenTx2Commits3
    Runnable removeAResourceFifteenMillRemoveOlivia = new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting Fifteen milli");
                Thread.sleep(15);
                Map<String, String> namePlace = DBMSImpl.Instance.getMap("tx1CommitsThenTx2Commits3", String.class, String .class);
                addToNotes(Thread.currentThread(), "About to remove \"Olivia\"");
                namePlace.remove("Olivia");
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };


    /**
     * 1] get Jim
     * 2] Wait 50 millisecs
     * 3] put <name,num> to the database
     * 4] commit
     * @param name
     * @param num
     * @return
     */
    Runnable genericGETNameNum(String name, int num)
    {
        return new Runnable()
    {
        @Override
        public void run()
        {
            try {
                TxMgrImpl.Instance.begin();
                addToNotes(Thread.currentThread(), "About to get a shadowDB");
                Map<String, Integer> firstLast = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
                addToNotes(Thread.currentThread(), "Just got the shadowDB, getting Jim");
                int age = firstLast.get("Jim");
                addToNotes(Thread.currentThread(), "Jim's age = " + age + " -- waiting fifty milli");
                Thread.sleep(50);
                addToNotes(Thread.currentThread(), "About to add \""+name+"\"");
                firstLast.put(name, num);
                addToNotes(Thread.currentThread(), "About to commit");
                TxMgrImpl.Instance.commit();
                addToNotes(Thread.currentThread(), "Commited");

            } catch (Exception e) {
                if (vPrintStackTrace) e.printStackTrace();
                addToNotes(Thread.currentThread(), "Exception caught = " + e.getLocalizedMessage());
            }
        }
    };
    }

    /////////////////////////////
    //  Mulit-Threaded Tests   //
    /////////////////////////////


    @BeforeClass
    public static void setUp() throws Exception
    {
        //1] Setup the map
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.createMap("nameAge", String.class, Integer.class);
            nameAge.put("Jim", 50);
            nameAge.put("Alex", 10);
            nameAge.put("Felix", 60);
            nameAge.put("Thomas", 1);
            nameAge.put("Henry", 73);
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception
    {
        if (TxMgrImpl.Instance.getTx().getCompletionStatus() == TxCompletionStatus.NOT_COMPLETED) {
            if (printWhy) logger.debug("I needed to rollback... why?");
            TxMgrImpl.Instance.rollback();
            printWhy = true;
        }
    }

    /**
     * Test that t1 gets a resource and holds onto it while t2 tries to grab it
     * t2 should throw an exception
     */
    @Test
    public void tx2AttemptToGetResourceTx1Has2()
    {
        if (vPrintName) logger.info("tx2AttemptToGetResourceTx1Has2");
        setPrintNote(true);
        DBMSImpl.Instance.setTxTimeoutInMillis(3);
        Thread t1 = new Thread(getAResource500Mill);
        Thread t2 = new Thread(getAResource500Mill);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            Thread.sleep(100);//give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (assertContains(t2, "Failed to get the lock"));
    }

    /**
     * tx1 gets a resource
     * tx2 tries to get the same resource
     * tx2 fails
     * afterwards tx1 adds a resource and commits
     * check the database afterwards that the commit happened
     */
    @Test
    public void tx2FailsandTx1Commits1()
    {
        DBMSImpl.Instance.setTxTimeoutInMillis(3);
        if (vPrintName) logger.debug("tx2FailsandTx1Commits1");
        Thread t1 = new Thread(getAResourceFifteenMillAndAddResourceSarah);
        Thread t2 = new Thread(getAResource500Mill);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                //give t1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(10);                               //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (assertContains(t2, "Failed to get the lock"));   //confirm that tx2 failed
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int sarahAge = nameAge.get("Sarah");
            assert (sarahAge == 34);                                   //confirm that tx1 committed its changes successfuly
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }

    }

    /**
     * tx1 gets a resource
     * tx2 tries to get the same resource
     * tx3 tries to get the same resource
     * tx2 fails
     * tx3 fails
     * afterwards tx1 adds a resource and commits
     * check the database afterwards that the commit happened
     */
    @Test
    public void ThreeTxsAndOnlyOneCompletes1()
    {
        DBMSImpl.Instance.setTxTimeoutInMillis(1);
        if (vPrintName) logger.debug("ThreeTxsAndOnlyOneCompletes1");
        setPrintNote(false);
        Thread t1 = new Thread(getAResourceFifteenMillAndAddResourceSarah);
        Thread t2 = new Thread(getAResource500Mill);
        Thread t3 = new Thread(getAResource500Mill);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t3);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        //t3//
        addToNotes(t3, "Starting t3");
        t3.start();
        addToNotes(t3, "Started t3");
        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            if (printNote) Thread.sleep(10);                   //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (assertContains(t2, "Failed to get the lock"));   //tx2 failed
        assert (assertContains(t3, "Failed to get the lock"));   //tx3 failed
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int sarahAge = nameAge.get("Sarah");
            assert (sarahAge == 34);                                   //tx1 completed properly
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }

    }

    /**
     * 1] Use the set txTimeout stuff to 50
     * 2] Tx1 should do everything, then commit, then tx2 should do everything
     */
    @Test
    public void txTwoWakesUpAfterTx1IsDone()
    {
        if (vPrintName) logger.debug("\n\n\n\ntxTwoWakesUpAfterTx1IsDone");
        setPrintNote(true);
        DBMSImpl.Instance.setTxTimeoutInMillis(50);
        Thread t1 = new Thread(getAResourceFifteenMillAndAddResourceSarah);
        Thread t2 = new Thread(getAResourceFifteenMillAndAddResourceRachel);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (!assertContains(t2, "Failed to get the lock"));      //tx2 didn't fail
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int sarahAge = nameAge.get("Sarah");
            assert (sarahAge == 34);                                       //tx1 worked properly
            int rachelAge = nameAge.get("Rachel");
            assert (rachelAge == 42);                                    //tx2 worked properly
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //  Proper Amount of waiting tests - you need to eyeball it, i.e. set tripleTryCheck=true at the the top of LockManager //
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    @Test
//-3- do a *put* in tx1, and therefore tx2 blocks and times out - verify that it only tried thrice ~ChangesLog.txt
    public void tx2FailsVerifyItTriedThrice1()
    {
        if (vPrintName) logger.debug("tx2FailsVerifyItTriedThrice1");
        setPrintNote(false);
        DBMSImpl.Instance.setTxTimeoutInMillis(1);
        Thread t1 = new Thread(getAResourceFiftyMillAndAddResourceHaily66);
        Thread t2 = new Thread(getAResourceFiftyMillAndAddResourceAshly78);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (assertContains(t2, "Failed to get the lock"));       //tx2 failed
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int hailyAge = nameAge.get("Haily");
            assert (hailyAge == 66);                                       //tx1 worked properly
            assertNull(nameAge.get("Ashly"));                             //tx2 didn't finish
            TxMgrImpl.Instance.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test//-4- do a *remove* in tx1, and therefore tx2 blocks and times out - verify that it only tried thrice
    public void tx2FailsVerifyItTriedThrice2()
    {
        if (vPrintName) logger.debug("tx2FailsVerifyItTriedThrice2");
        setPrintNote(false);
        setvPrintStackTrace(false);
        DBMSImpl.Instance.setTxTimeoutInMillis(1);
        Thread t1 = new Thread(deleteAResourceFiftyMillAndAddResourceSarit);
        Thread t2 = new Thread(deleteAResourceFiftyMillAndAddResourceRebecca);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (assertContains(t2, "Failed to get the lock"));       //tx2 failed
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int saritAge = nameAge.get("Sarit");
            assert (saritAge == 77);                                     //tx1 worked properly
            assertNull(nameAge.get("Rebecca"));                          //tx2 didn't finish
            TxMgrImpl.Instance.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            setvPrintStackTrace(false);
        }
    }

    @Test//-5- do a *get* in tx1, and therefore tx2 blocks and times out - verify that it only tried thrice
    public void tx2FailsVerifyItTriedThrice3()
    {
        if (vPrintName) logger.debug("tx2FailsVerifyItTriedThrice3");
        setPrintNote(false);
        setvPrintStackTrace(false);
        DBMSImpl.Instance.setTxTimeoutInMillis(1);
        Thread t1 = new Thread(genericGETNameNum("Nynave", 44));
        Thread t2 = new Thread(genericGETNameNum("Egwane", 102));

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        assert (assertContains(t2, "Failed to get the lock"));       //tx2 failed
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int nynaveAge = nameAge.get("Nynave");
            assert (nynaveAge == 44);                                     //tx1 worked properly
            assertNull(nameAge.get("Egwane"));                            //tx2 didn't finish
            TxMgrImpl.Instance.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            setvPrintStackTrace(false);
        }
    }


    @Test//-6- do a *put* in tx1, and tx2 blocks but can complete itself as well
    public void tx1CommitsThenTx2Commits1()
    {
        if (vPrintName) logger.debug("tx1CommitsThenTx2Commits1");
        setPrintNote(true);
        setvPrintStackTrace(true);
        DBMSImpl.Instance.setTxTimeoutInMillis(500);//<-- by default it has plenty of time
        Thread t1 = new Thread(putAResource30MillAndAddResourceHanna22andHoward55);
        Thread t2 = new Thread(putAResource30MillAndAddResourceMiley33andHoward56);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int howardAge = nameAge.get("Howard");
            int hannaAge = nameAge.get("Hanna");
            int mileyAge = nameAge.get("Miley");
            assertEquals(howardAge, 56);
            assertEquals(hannaAge, 22);
            assertEquals(mileyAge, 33);
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }

    }
    @Test//-7- do a *get* in tx1, and tx2 blocks but can complete itself as well
    public void tx1CommitsThenTx2Commits2()
    {
        if (vPrintName) logger.debug("tx1CommitsThenTx2Commits2");
        setPrintNote(false);
        setvPrintStackTrace(false);
        DBMSImpl.Instance.setTxTimeoutInMillis(1000);//<-- by default it has plenty of time
        Thread t1 = new Thread(genericGETNameNum("Shayna", 12));
        Thread t2 = new Thread(genericGETNameNum("Shaindy", 13));

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> nameAge = DBMSImpl.Instance.getMap("nameAge", String.class, Integer.class);
            int shaynaAge = nameAge.get("Shayna");                      //this should go through
            int shaindyAge = nameAge.get("Shaindy");                    //this should go through as well
            assertEquals(shaynaAge, 12);
            assertEquals(shaindyAge, 13);
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }
    @Test//-8- do a remove in tx1, and tx2 blocks but can complete itself as well
    public void tx1CommitsThenTx2Commits3()
    {
        if (vPrintName) logger.debug("tx1CommitsThenTx2Commits3");
        setPrintNote(false);
        setvPrintStackTrace(false);
        DBMSImpl.Instance.setTxTimeoutInMillis(1000);//<-- by default it has plenty of time
        Thread t1 = new Thread(removeAResourceFifteenMillRemoveEmma);
        Thread t2 = new Thread(removeAResourceFifteenMillRemoveOlivia);

        WhiteBoard.Instance.resetNotes();
        WhiteBoard.Instance.setUp_AddThreadToNotes(t1);
        WhiteBoard.Instance.setUp_AddThreadToNotes(t2);

        try {
            TxMgrImpl.Instance.begin();
            Map<String, String> namePlace = DBMSImpl.Instance.createMap("tx1CommitsThenTx2Commits3", String.class, String.class);
            namePlace.put("Emma", "NY");
            namePlace.put("Olivia", "NJ");
            namePlace.put("Sophia", "PENN");
            TxMgrImpl.Instance.commit();
        } catch (RollbackException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        }
        addToNotes(t1, "Starting t1");
        t1.start();
        addToNotes(t1, "Started t1");
        try {
            Thread.sleep(5);                                    //give tx1 a head start
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //t2//
        addToNotes(t2, "Starting t2");
        t2.start();
        addToNotes(t2, "Started t2");
        try {
            t1.join();
            t2.join();
            if (printNote) Thread.sleep(100);                     //give time for all the notes to be written
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (printNote) printNotes();
        try {
            TxMgrImpl.Instance.begin();
            Map<String, String> namePlace = DBMSImpl.Instance.getMap("tx1CommitsThenTx2Commits3", String.class, String.class);
            String sophia = namePlace.get("Sophia");
            String olivia = namePlace.get("Olivia");
            String Emma = namePlace.get("Emma");
            assertEquals(sophia, "PENN");
            assertNull(olivia);
            assertNull(Emma);
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }





    //////////////////////////////
    //  Single Threaded Stuff   //
    //////////////////////////////
    class Fun
    {
        public String yes = "yes";

        public Fun(String s)
        {
            yes = s;
        }
    }

    @Test(expected = SystemException.class)
    public void serializationTest1() throws RollbackException, SystemException, NotSupportedException
    {
        if (vPrintName) logger.debug("serializationTest1");
        TxMgrImpl.Instance.begin();
        Map<String, Fun> fun = DBMSImpl.Instance.createMap("Fun", String.class, Fun.class);
        Fun funny = new Fun("Hey");
        fun.put("funnyStr", funny);
        TxMgrImpl.Instance.commit();
    }

    /**
     * Test that the database won't accept a change if there was a non-serializable item in the tx
     *
     * @throws RollbackException
     * @throws SystemException
     * @throws NotSupportedException
     */
    @Test
    public void serializationTest2() throws RollbackException, SystemException, NotSupportedException
    {
        boolean caughtSystemException = false;
        boolean caughtNoSuchElementException = false;
        if (vPrintName) logger.debug("serializationTest2");
        TxMgrImpl.Instance.begin();
        Map<String, Fun> fun = DBMSImpl.Instance.createMap("Fun", String.class, Fun.class);
        Fun funny = new Fun("Hey");
        fun.put("funnyStr", funny);
        Map<String, String> serializationTest2 = DBMSImpl.Instance.createMap("serializationTest2", String.class, String.class);
        serializationTest2.put("John", "Cohn");
        try {
            TxMgrImpl.Instance.commit();
        } catch (SystemException e) {
            caughtSystemException = true;
        }
        assert (caughtSystemException);
        try {
            TxMgrImpl.Instance.begin();
            DBMSImpl.Instance.getMap("serializationTest2", String.class, String.class);
            TxMgrImpl.Instance.commit();
        } catch (NoSuchElementException e) {
            caughtNoSuchElementException = true;
            printWhy = false;
        }
        assert (caughtNoSuchElementException);
    }

    /**
     * Make sure that if I commit a delete it works, i.e.
     * 1] Put Cali, and NY in  - and commit
     * 2] Delete NY, add NJ  - and commit
     * 3] Check for NY and it should be null, but NJ should exist
     */
    @Test
    public void deletionWorks1()
    {
        if (vPrintName) logger.debug("deletionWorks1");
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> cityPop = DBMSImpl.Instance.createMap("deletionWorks1", String.class, Integer.class);
            cityPop.put("Cali", 200);
            cityPop.put("NY", 100);
            TxMgrImpl.Instance.commit();
            //now delete
            TxMgrImpl.Instance.begin();
            cityPop = DBMSImpl.Instance.getMap("deletionWorks1", String.class, Integer.class);
            cityPop.put("NJ", 500);
            cityPop.remove("NY");
            TxMgrImpl.Instance.commit();
            //now check that it worked
            TxMgrImpl.Instance.begin();
            cityPop = DBMSImpl.Instance.getMap("deletionWorks1", String.class, Integer.class);
            assertNull(cityPop.get("NY"));
            int caliNum = cityPop.get("Cali");
            int nJNum = cityPop.get("NJ");
            assertEquals(caliNum, 200);
            assertEquals(nJNum, 500);
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    /**
     * A delete and then put in 1 tx should work properly
     */
    @Test
    public void deletionWorks2()
    {
        if (vPrintName) logger.debug("deletionWorks2");
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> cityPop = DBMSImpl.Instance.createMap("deletionWorks2", String.class, Integer.class);
            cityPop.put("Cali", 200);
            cityPop.put("NY", 100);
            TxMgrImpl.Instance.commit();
            //now delete
            TxMgrImpl.Instance.begin();
            cityPop = DBMSImpl.Instance.getMap("deletionWorks1", String.class, Integer.class);
            cityPop.put("NJ", 500);
            cityPop.remove("NJ");
            cityPop.put("NJ", 500);
            cityPop.remove("NY");
            cityPop.remove("Cali");
            cityPop.put("NY", 150);
            cityPop.put("Cali", 200);
            TxMgrImpl.Instance.commit();
            //now check that it worked
            TxMgrImpl.Instance.begin();
            cityPop = DBMSImpl.Instance.getMap("deletionWorks1", String.class, Integer.class);
            int caliNum = cityPop.get("Cali");
            int nJNum = cityPop.get("NJ");
            int nyNum = cityPop.get("NY");
            assertEquals(nyNum, 150);
            assertEquals(caliNum, 200);
            assertEquals(nJNum, 500);
            TxMgrImpl.Instance.commit();
        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }
}