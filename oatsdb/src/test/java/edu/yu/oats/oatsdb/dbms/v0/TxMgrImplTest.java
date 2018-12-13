package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.*;

public class TxMgrImplTest
{
    TxMgrImpl theTxMgr = TxMgrImpl.Instance;
    DBMSImpl theDBMS = DBMSImpl.Instance;

    @After
    public void setUp() throws Exception
    {
        if(TxMgrImpl.Instance.getStatus().equals(TxStatus.ACTIVE)) TxMgrImpl.Instance.commit();
    }

    //////////////////////////////////////////
    //  All DBMS Methods Must be in a Tx    //
    //////////////////////////////////////////

    /////////////////////////
    //  1] createMap Tests //
    /////////////////////////

    @Test
    public void createMapInsideTxAndCommitTest1()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest1", String.class, Integer.class);
            TxMgrImpl.Instance.commit();
        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void createMapInsideTxAndCommitTest2()//check that the statuses worked
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest2", String.class, Integer.class);
            //new stuff
            TxStatus status = TxMgrImpl.Instance.getStatus();
            Tx trans = TxMgrImpl.Instance.getTx();

            assertEquals(status, TxStatus.ACTIVE);
            assertEquals(trans.getCompletionStatus(), TxCompletionStatus.NOT_COMPLETED);

            TxMgrImpl.Instance.commit();

            assertEquals(trans.getStatus(), TxStatus.COMMITTED);
            assertEquals(trans.getCompletionStatus(), TxCompletionStatus.COMMITTED);

        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void createMapInsideTxAndCommitTest3() //confirm there is no transaction after it's committed
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest3", String.class, Integer.class);
            TxMgrImpl.Instance.commit();
            assertEquals(TxStatus.NO_TRANSACTION, TxMgrImpl.Instance.getStatus());

        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void createMapInsideTxAndCommitTest4()//commit and retry
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest4", String.class, Integer.class);
            Tx tx = TxMgrImpl.Instance.getTx();
            assertEquals(TxStatus.ACTIVE, tx.getStatus());
            assertEquals(TxStatus.ACTIVE, TxMgrImpl.Instance.getStatus());

            TxMgrImpl.Instance.commit();
            assertEquals(TxStatus.COMMITTED, tx.getStatus());
            assertEquals(TxStatus.NO_TRANSACTION, TxMgrImpl.Instance.getStatus());

            TxMgrImpl.Instance.begin();
            assertEquals(TxStatus.ACTIVE, TxMgrImpl.Instance.getStatus());
            Map<String, Integer> map2 = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest41", String.class, Integer.class);

            TxMgrImpl.Instance.commit();
            assertEquals(TxStatus.NO_TRANSACTION, TxMgrImpl.Instance.getStatus());

        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void createMapInsideTxAndCommitTest5()//commit completion status
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest5", String.class, Integer.class);
            Tx tx = TxMgrImpl.Instance.getTx();
            assertEquals(TxStatus.ACTIVE, tx.getStatus());
            assertEquals(TxStatus.ACTIVE, TxMgrImpl.Instance.getStatus());
            assertEquals(TxCompletionStatus.NOT_COMPLETED, tx.getCompletionStatus());

            TxMgrImpl.Instance.commit();
            assertEquals(TxStatus.COMMITTED, tx.getStatus());
            assertEquals(TxStatus.NO_TRANSACTION, TxMgrImpl.Instance.getStatus());
            assertEquals(TxCompletionStatus.COMMITTED, tx.getCompletionStatus());

            TxMgrImpl.Instance.begin();
            assertEquals(TxStatus.ACTIVE, TxMgrImpl.Instance.getStatus());
            Map<String, Integer> map2 = DBMSImpl.Instance.createMap("createMapInsideTxAndCommitTest51", String.class, Integer.class);
            Tx tx2 = theTxMgr.getTx();

            TxMgrImpl.Instance.rollback();
            assertEquals(TxStatus.NO_TRANSACTION, TxMgrImpl.Instance.getStatus());
            assertEquals(TxCompletionStatus.ROLLEDBACK, tx2.getCompletionStatus());

        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createMapInsideTxAndRollbackTest1()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndRollbackTest1", String.class, Integer.class);
            TxMgrImpl.Instance.rollback();
        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void createMapInsideTxAndRollbackTest2()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapInsideTxAndRollbackTest2", String.class, Integer.class);
            //new stuff
            TxStatus status = TxMgrImpl.Instance.getStatus();
            Tx trans = TxMgrImpl.Instance.getTx();

            assertEquals(status, TxStatus.ACTIVE);
            assertEquals(trans.getCompletionStatus(), TxCompletionStatus.NOT_COMPLETED);

            TxMgrImpl.Instance.rollback();

            assertEquals(trans.getStatus(), TxStatus.ROLLEDBACK);
            assertEquals(trans.getCompletionStatus(), TxCompletionStatus.ROLLEDBACK);

        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        }

    }

    @Test(expected = ClientNotInTxException.class)
    public void createMapOutsideTxTest1()
    {
        Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapOutsideTxTest1", String.class, Integer.class);
    }

    @Test(expected = ClientNotInTxException.class)
    public void createMapLeaveTxAndUpdateTheMapTest1()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> map = DBMSImpl.Instance.createMap("createMapLeaveTxAndUpdateTheMapTest1", String.class, Integer.class);
            TxMgrImpl.Instance.commit();
            map.put("Hey", new Integer(7));
        }catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }
    }

    ////////////////////////////////////////////////
    //  2] txMgrImpl begin, commit, rollback test //
    ////////////////////////////////////////////////

    @Test
    public void txMgrImplJustBeginTest1() throws SystemException, NotSupportedException
    {
        TxMgrImpl.Instance.begin();
    }
    @Test(expected = NotSupportedException.class)
    public void txMgrImplDoubleBeginTest1() throws NotSupportedException, SystemException
    {
        TxMgrImpl.Instance.begin();
        TxMgrImpl.Instance.begin();
    }
    @Test(expected = IllegalStateException.class)
    public void txMgrImplJustCommitTest1() throws RollbackException, SystemException
    {
        TxMgrImpl.Instance.commit();
    }
    @Test(expected = IllegalStateException.class)
    public void txMgrImplJustRollBackTest1() throws SystemException
    {
        TxMgrImpl.Instance.rollback();
    }
    @Test(expected = IllegalStateException.class)
    public void txMgrImplCommitRollBackTest1() throws SystemException, RollbackException, NotSupportedException
    {
        TxMgrImpl.Instance.begin();
        TxMgrImpl.Instance.commit();
        TxMgrImpl.Instance.rollback();

    }
    @Test(expected = IllegalStateException.class)
    public void txMgrImplRollBackCommittTest1() throws SystemException, RollbackException, NotSupportedException
    {
        TxMgrImpl.Instance.begin();
        TxMgrImpl.Instance.rollback();
        TxMgrImpl.Instance.commit();

    }


    /////////////////////////////////////////
    //  All Map Methods Must be in a Tx    //
    /////////////////////////////////////////


    @Test
    public void mapMustBeInTxTest1()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> outOfTxMap = DBMSImpl.Instance.createMap("TestMap", String.class, Integer.class);
            outOfTxMap.put("Moshe", 21);
            Map<String, Integer> outOfTxMap2 = theDBMS.getMap("TestMap", String.class, Integer.class);
            assertEquals(outOfTxMap, outOfTxMap2);
            assert (outOfTxMap.containsKey("Moshe"));
            assertEquals(outOfTxMap.get("Moshe"), new Integer(21));
            theTxMgr.commit();


        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }

    }
    @Test(expected = ClientNotInTxException.class)
    public void mapMustBeInTxTest2()
    {
        try {
            TxMgrImpl.Instance.begin();
            Map<String, Integer> outOfTxMap = DBMSImpl.Instance.createMap("TestMap2", String.class, Integer.class);
            outOfTxMap.put("Moshe", 21);
            Map<String, Integer> outOfTxMap2 = theDBMS.getMap("TestMap2", String.class, Integer.class);
            assertEquals(outOfTxMap, outOfTxMap2);
            assert (outOfTxMap.containsKey("Moshe"));
            assertEquals(outOfTxMap.get("Moshe"), new Integer(21));
            theTxMgr.commit();

            outOfTxMap.get("TestMap");
            //outOfTxMap.put("Chayim", 23);
            //assertEquals(outOfTxMap, outOfTxMap2);

        } catch (NotSupportedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (RollbackException e) {
            e.printStackTrace();
        }

    }
}