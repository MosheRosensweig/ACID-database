package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.SystemException;
import edu.yu.oats.oatsdb.dbms.Tx;
import edu.yu.oats.oatsdb.dbms.TxCompletionStatus;
import edu.yu.oats.oatsdb.dbms.TxStatus;

import java.io.Serializable;

/**
 *
 * @author mosherosensweig
 * @version 10/11/18 7pm
 */
class TxImpl implements Tx, Serializable
{
    private TxStatus status;
    private TxCompletionStatus completionStatus;

    public TxImpl(TxStatus txStatus)
    {
        status = txStatus;
        completionStatus = TxCompletionStatus.NOT_COMPLETED;
        if(status == TxStatus.COMMITTED || status == TxStatus.ROLLEDBACK)
            completionStatus = TxCompletionStatus.updateTxStatus(completionStatus, status);
        else if(txStatus == TxStatus.NO_TRANSACTION) completionStatus = null;
    }
    /** Obtain the status of the transaction associated with the target
     * Transaction object.
     *
     * @return Appropriate TxStatus enum value.
     * @throws SystemException Thrown if the transaction manager encounters an
     * unexpected error condition.
     */
    public TxStatus getStatus() throws SystemException
    {
        return status;
    }

    /** Obtain the "completion" status of the transaction.
     *
     * @returns The completed transaction status.  If the transaction has not yet
     * completed, returns the "ACTIVE" value.  Otherwise, even if the transaction
     * is no longer associated with the current (or any other) thread, returns
     * the appropriate "completion" value.  Will not throw an exception.
     */
    public TxCompletionStatus getCompletionStatus()
    {
        return completionStatus;
    }

    protected void setStatus(TxStatus newStatus)
    {
        status = newStatus;
        if(status == TxStatus.COMMITTED || status == TxStatus.ROLLEDBACK)
            completionStatus = TxCompletionStatus.updateTxStatus(completionStatus, status);
    }
}
