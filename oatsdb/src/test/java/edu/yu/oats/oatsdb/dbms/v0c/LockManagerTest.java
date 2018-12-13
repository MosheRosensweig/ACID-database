package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class LockManagerTest
{
    private static Logger logger = LogManager.getLogger();
    //private static DBMSImpl dbms = DBMSImpl.Instance;
    //private static TxMgrImpl txmgr = TxMgrImpl.Instance;

    @After
    public void commit()
    {
        try {
            if(!TxMgrImpl.Instance.getStatus().equals(TxStatus.NO_TRANSACTION)) TxMgrImpl.Instance.commit();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    @Test
    /**
     * 1] Make sure that changes continue to exist after a commit
     * 2] Make sure that it can tell when a lock is taken or it already has a lock
     */
    public void lockManagerCommitTest1()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, String> teachers = DBMSImpl.Instance.createMap("Teachers1", String.class, String.class);
            teachers.put("John","history" );
            teachers.put("Adam", "Talmud");
            teachers.get(("John"));
            teachers.get("Adam");
            TxMgrImpl.Instance.commit();

            logger.debug("Starting second transaction");
            TxMgrImpl.Instance.begin();
            Map<String, String> teachers2 = DBMSImpl.Instance.getMap("Teachers1", String.class, String.class);
            teachers2.get("John");
            teachers2.get("Adam");
            logger.debug("get them again");
            teachers2.get("John");
            teachers2.get("Adam");
            logger.debug("get them a third time, after removing them");
            teachers2.remove("John");
            teachers2.remove("Adam");
            teachers2.get("John");
            teachers2.get("Adam");


        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    @Test
    /**
     * 1] Make sure that if a new Tx uses the old Tx's table it won't affect the real db
     * 2] Make sure that transaction objects change from transaction to transaction
     * This is the same as lockManagerCommitTest3, but this time I assume I cannot
     * use a map from a previous transaction
     *
     */
            //(expected = ClientNotInTxException.class)
    public void lockManagerCommitTest3()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, String> teachers = DBMSImpl.Instance.createMap("Teachers2", String.class, String.class);
            logger.debug("CONFIRMATION - the hashcode for the Teachers2 table is = " +teachers.hashCode());
            teachers.put("John","history" );
            teachers.put("Adam", "Talmud");
            Tx tx = TxMgrImpl.Instance.getTx();
            TxMgrImpl.Instance.commit();
            logger.debug(tx.getStatus());

            logger.debug("Starting second transaction");
            TxMgrImpl.Instance.begin();
            logger.debug("CONFIRMATION - (second tx) the hashcode for the Teachers2 table is = " +teachers.hashCode());
            teachers.get("John");
            teachers.get("Adam");
            Tx tx1 = TxMgrImpl.Instance.getTx();
            assertNotEquals(tx, tx1);

            teachers.put("Paul", "English");
            TxMgrImpl.Instance.commit();

            logger.debug("Starting 3rd transaction");
            TxMgrImpl.Instance.begin();
            Map<String, String> teachers2 = DBMSImpl.Instance.getMap("Teachers2", String.class, String.class);
            logger.debug("CONFIRMATION - the hashcode for the Teachers2 table is = " +teachers.hashCode());
            logger.debug("CONFIRMATION - the hashcode for the Teachers2 v2 table is = " +teachers2.hashCode());
            assertEquals(teachers2.size(), 0);
            assertEquals(teachers2.get("Paul"), "English");
            assertEquals(teachers2.size(), 1);//the shadowMap should have 1 thing now
            assertEquals(teachers.size(), 1);//the shadowMap should have 1 thing now even though this is a different proxy
            assertEquals(teachers.get("Paul"), "English");
            assertEquals(teachers.get("Paul"), teachers2.get("Paul"));
            TxMgrImpl.Instance.commit();

        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void lockManagerRollbackTest1()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, String> teachers = DBMSImpl.Instance.createMap("Teachers3", String.class, String.class);
            teachers.put("John","history" );
            teachers.put("Adam", "Talmud");
            teachers.get(("John"));
            teachers.get("Adam");
            TxMgrImpl.Instance.rollback();

            logger.debug("Starting second transaction");
            TxMgrImpl.Instance.begin();
            Map<String, String> teachers2 = DBMSImpl.Instance.getMap("Teachers3", String.class, String.class);
            assertNull(teachers2);
            TxMgrImpl.Instance.begin();


        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        }

    }

}