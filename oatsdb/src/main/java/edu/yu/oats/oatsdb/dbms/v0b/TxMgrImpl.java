package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mosherosensweig
 * @version 10/4/18 8pm
 */
public enum TxMgrImpl implements TxMgr
{
    Instance;
    //////////////////////
    //  Logger Stuff    //
    //////////////////////
    private static Logger logger = LogManager.getLogger();
    private final static boolean vTest = false;
    private final static boolean leffTestLog = true;
    /////////////////////////////
    //  End of Logger Stuff    //
    /////////////////////////////
    private static Map<Thread, Tx> activeThreads = new ConcurrentHashMap<Thread, Tx>();

    /**
     * Create a new transaction and associate it with the current thread.
     *
     * @throws NotSupportedException Thrown if the thread is already associated
     *                               with a transaction and the Transaction Manager implementation does not
     *                               support nested transactions.
     * @throws SystemException       Thrown if the transaction manager encounters an
     *                               unexpected error condition.
     */
    public void begin() throws NotSupportedException, SystemException
    {
        if(leffTestLog)logger.debug("TESTING - beginning a tx");
        Thread thisThread = Thread.currentThread();
        if (activeThreads.containsKey(thisThread))
            throw new NotSupportedException("Current thread is already in a transaction");
        activeThreads.put(thisThread, new TxImpl(TxStatus.ACTIVE));
        DBMSImpl.Instance.tx.set(activeThreads.get(thisThread));
    }

    /**
     * Complete the transaction associated with the current thread. When this
     * method completes, the thread is no longer associated with a transaction.
     *
     * @throws RollbackException     Thrown to indicate that the transaction has been
     *                               rolled back rather than committed.
     * @throws IllegalStateException Thrown if the current thread is not
     *                               associated with a transaction.
     * @throws SystemException       Thrown if the transaction manager encounters an
     *                               unexpected error condition.
     */
    public void commit() throws RollbackException, IllegalStateException, SystemException
    {
        if(leffTestLog || vTest)logger.debug("TESTING - start committing a tx");
        Thread currThr = Thread.currentThread();
        if (!activeThreads.containsKey(currThr))
            throw new IllegalStateException("Current thread is not associated with a transaction");
        //TxImpl committedTx = (TxImpl) activeThreads.remove(currThr);
        TxImpl committedTx = (TxImpl) activeThreads.get(currThr); //(TxImpl) activeThreads.remove(currThr);

        //  Change the status of the transaction //
        committedTx.setStatus(TxStatus.COMMITTING);
        try {
            DBMSImpl.Instance.commit();
        } catch (Exception e) {
            if(leffTestLog)logger.debug("TESTING - COMMITTING ISSUE --> most likely something was not serializable");
            rollback();
            throw new SystemException("Commit failed : "+e.getMessage());
        }
        cleanUpLocks();
        committedTx.setStatus(TxStatus.COMMITTED);
        activeThreads.remove(currThr);
        DBMSImpl.Instance.tx.set(null); //reset the DBMS transaction
        if(leffTestLog)logger.debug("TESTING - end committing a tx");
    }

    /**
     * Roll back the transaction associated with the current thread. When this
     * method completes, the thread is no longer associated with a transaction.
     *
     * @throws IllegalStateException Thrown if the current thread is not
     *                               associated with a transaction.
     * @throws SystemException       Thrown if the transaction manager encounters an
     *                               unexpected error condition.
     */
    public void rollback() throws IllegalStateException, SystemException
    {
        if(leffTestLog)logger.debug("TESTING - start rolling back a tx");
        Thread currThr = Thread.currentThread();
        if (!activeThreads.containsKey(currThr))
            throw new IllegalStateException("Current thread is not associated with a transaction");
        TxImpl rolledBackTx = (TxImpl) activeThreads.get(Thread.currentThread()); //(TxImpl) activeThreads.remove(currThr);

        //  Change the status of the transaction //
        rolledBackTx.setStatus(TxStatus.ROLLING_BACK);
        DBMSImpl.Instance.rollback();
        cleanUpLocks();
        rolledBackTx.setStatus(TxStatus.ROLLEDBACK);
        activeThreads.remove(currThr);
        DBMSImpl.Instance.tx.set(null); //reset the DBMS transaction
        if(leffTestLog)logger.debug("TESTING - end committing a tx");
    }

    /**
     * Return the transaction object that represents the transaction context of the
     * calling thread.  If no transaction is associated with the current thread,
     * returns a Transaction whose getStatus() equals TxStatus.NO_TRANSACTION.
     *
     * @throws SystemException Thrown if the transaction manager encounters an
     *                         unexpected error condition.
     */
    public Tx getTx() throws SystemException
    {
        Thread currThread = Thread.currentThread();
        Tx thisThreadsTx;
        if (activeThreads.containsKey(currThread)) thisThreadsTx = activeThreads.get(currThread);
        else thisThreadsTx = new TxImpl(TxStatus.NO_TRANSACTION);
        return thisThreadsTx;
    }

    /**
     * Obtain the status of the transaction associated with the current thread.
     * <p>
     * Possible states: UNKNOWN, ACTIVE, COMMITTED, COMMITTING, ROLLEDBACK, ROLLING_BACK, NO_TRANSACTION
     *
     * @throws SystemException Thrown if the transaction manager encounters an
     *                         unexpected error condition.
     * @returns The transaction status. If no transaction is associated with the
     * current thread, this method returns the TxStatus.NO_TRANSACTION value
     */
    public TxStatus getStatus() throws SystemException
    {
        TxStatus status = TxStatus.NO_TRANSACTION;
        Thread currentThread = Thread.currentThread();
        if (activeThreads.containsKey(currentThread)) status = activeThreads.get(currentThread).getStatus();
        return status;
    }

    private void cleanUpLocks()
    {
        LockManager.Instance.clearTxsLocks();
    }
}
