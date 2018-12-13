package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author mosherosensweig
 * @version 10/4/18 8pm
 */
public enum TxMgrImpl implements TxMgr
{
    Instance;
    private static Map<Thread, Tx> activeThreads = new ConcurrentHashMap<Thread, Tx>();

    /** Create a new transaction and associate it with the current thread.
     *
     * @throws NotSupportedException Thrown if the thread is already associated
     * with a transaction and the Transaction Manager implementation does not
     * support nested transactions.
     * @throws SystemException Thrown if the transaction manager encounters an
     * unexpected error condition.
     */
    public void begin() throws NotSupportedException, SystemException
    {
        Thread thisThread = Thread.currentThread();
        if(activeThreads.containsKey(thisThread)) throw new NotSupportedException("Current thread is already in a transaction");
        activeThreads.put(thisThread, new TxImpl(TxStatus.ACTIVE));
    }

    /** Complete the transaction associated with the current thread. When this
     * method completes, the thread is no longer associated with a transaction.
     *
     * @throws RollbackException Thrown to indicate that the transaction has been
     * rolled back rather than committed.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     * @throws SystemException Thrown if the transaction manager encounters an
     * unexpected error condition.
     */
    public void commit() throws RollbackException, IllegalStateException, SystemException
    {
        Thread currThr = Thread.currentThread();
        if(!activeThreads.containsKey(currThr)) throw new IllegalStateException("Current thread is not associated with a transaction");
        TxImpl committedTx = (TxImpl) activeThreads.remove(currThr);
        //I don't know how there could be a transaction still in "activeThreads" that has a status "rolledback"
        //but this is what the api requires
        if(committedTx.getStatus() == TxStatus.ROLLEDBACK) throw new RollbackException("Attempted to commit a rolled back transaction");

        //  Change the status of the transaction //
        committedTx.setStatus(TxStatus.COMMITTING);
        //TODO - implement this stuff
        committedTx.setStatus(TxStatus.COMMITTED);
    }

    /** Roll back the transaction associated with the current thread. When this
     * method completes, the thread is no longer associated with a transaction.
     *
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     * @throws SystemException Thrown if the transaction manager encounters an
     * unexpected error condition.
     */
    public void rollback() throws IllegalStateException, SystemException
    {
        Thread currThr = Thread.currentThread();
        if(!activeThreads.containsKey(currThr)) throw new IllegalStateException("Current thread is not associated with a transaction");
        TxImpl rolledBackTx = (TxImpl) activeThreads.remove(currThr);

        //  Change the status of the transaction //
        rolledBackTx.setStatus(TxStatus.ROLLING_BACK);
        //TODO - implement this stuff
        rolledBackTx.setStatus(TxStatus.ROLLEDBACK);
    }

    /** Return the transaction object that represents the transaction context of the
     * calling thread.  If no transaction is associated with the current thread,
     * returns a Transaction whose getStatus() equals TxStatus.NO_TRANSACTION.
     *
     * @throws SystemException Thrown if the transaction manager encounters an
     * unexpected error condition.
     */
    public Tx getTx() throws SystemException
    {
        Thread currThread = Thread.currentThread();
        Tx thisThreadsTx;
        if(activeThreads.containsKey(currThread)) thisThreadsTx = activeThreads.get(currThread);
        else thisThreadsTx = new TxImpl(TxStatus.NO_TRANSACTION);
        return thisThreadsTx;
    }

    /** Obtain the status of the transaction associated with the current thread.
     *
     * Possible states: UNKNOWN, ACTIVE, COMMITTED, COMMITTING, ROLLEDBACK, ROLLING_BACK, NO_TRANSACTION
     *
     * @returns The transaction status. If no transaction is associated with the
     * current thread, this method returns the TxStatus.NO_TRANSACTION value
     * @throws SystemException Thrown if the transaction manager encounters an
     * unexpected error condition.
     */
    public TxStatus getStatus() throws SystemException
    {
        TxStatus status = TxStatus.NO_TRANSACTION;
        Thread currentThread = Thread.currentThread();
        if(activeThreads.containsKey(currentThread)) status = activeThreads.get(currentThread).getStatus();
        return status;
    }
}
