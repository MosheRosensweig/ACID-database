package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.ClientTxRolledBackException;
import edu.yu.oats.oatsdb.dbms.SystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is for version c
 * <p>
 * A transaction can call 2 methods here:
 * <p>
 * [1] getResourceLock - blocks until it gets the resource
 * [2] clearTxsLocks - clear the current Tx's locks
 *
 * @author mosherosensweig
 * @version 10/21/18 AM
 */
public enum LockManager
{
    Instance;
    //////////////////////
    //  Logger Stuff    //
    //////////////////////
    private static Logger logger = LogManager.getLogger();
    private final static boolean verbose = false;       // used for controlling the logger
    private final static boolean vTest = false;         // used for controlling the logger
    private final static boolean tripleTryCheck = false; // used to prove that the lockManager tries to get the lock 3 times, but only sleeps twice
    private final static boolean leffTestLog = true; //used for Leff's tests
    //TODO -  change "thread" to "Tx" - maybe

    //Number of atttempts to grab a lock before rolling back
    private final int NUMBER_OF_ATTEMPTS = 2; //<-- v0b is allowed up to 3 checks
    //General storage of all locks
    //thread-safe collection that stores tableName-Key-Thread
    private ConcurrentHashMap<String, ConcurrentHashMap<?, Thread>> globalLocks = new ConcurrentHashMap<>();

    //Threadlocal storage of locks for committing and rollback purposes - to enable easy unlocking of everything
    //these are stored as <tableName,set(of locked keys)>
    private ThreadLocal<ConcurrentHashMap<String, Set<?>>> localLocks = ThreadLocal.withInitial(ConcurrentHashMap::new);

    //List of threads waiting for this thread to commit or rollback (for version c)
    private ConcurrentHashMap<Thread, Set<Thread>> waitingLists = new ConcurrentHashMap<>();

    //This is used to pass information to the .get() call
    //If the I just got the lock, return true
    ThreadLocal<Boolean> iJustGotTheLock = ThreadLocal.withInitial(() -> new Boolean(false));
    //TODO - List of table names to enable locking tablenames

    private void intializeThreadLocalLocks()
    {
        localLocks.set(new ConcurrentHashMap<>());
    }

    private void intializeThreadLocalGotLocks()
    {
        iJustGotTheLock = new ThreadLocal<>();
        iJustGotTheLock.set(false);
    }

    /**
     * For blocking the pattern is this:
     * Check#1, sleep#1, Check#2, break
     * @param tableName
     * @param key
     * @param <K>
     * @return If I just got the lock return true, if I already had the lock return false
     * @throws InterruptedException
     * @throws SystemException
     */
    <K> boolean getResourceLock(String tableName, K key) throws InterruptedException, SystemException
    {
        if(leffTestLog)logger.debug("TESTING - getting lock for table = "+tableName+", key = "+key);
        iJustGotTheLock.set(false);
        int numberOfAttempts = 0;  //this means the number of attempts so far.
        Thread currThread = Thread.currentThread();
        boolean gotLock = false;
        while (!gotLock && (numberOfAttempts++ < NUMBER_OF_ATTEMPTS)) { //keep trying to get the lock until I get it, or I pass the number of allowed attempts
            if(tripleTryCheck)logger.debug("TESTING -Checking attempt #"+(numberOfAttempts));
            Thread lockingThread = tryToLockResource(tableName, key);
            if (lockingThread.equals(currThread)) {                     //if I successfully locked it, exit
                gotLock = true;
                if(leffTestLog)logger.debug("TESTING - I successfully got the lock for table = "+tableName+", key = "+key);
            } else {                                                    //if someone else has the lock
                if(leffTestLog)logger.debug("TESTING - I did not get the lock. This was attempt "+numberOfAttempts +
                        "/2, for table = "+tableName+", key = "+key);
                if(numberOfAttempts == NUMBER_OF_ATTEMPTS) break;       //it can only sleep once (v0c), per these weird requirements
                addToWaitList(lockingThread, currThread);               //tell the locking thread to wake me up when he's done
                if(tripleTryCheck)logger.debug("TESTING - Going To Sleep#"+(numberOfAttempts));
                try {
                    Thread.sleep(DBMSImpl.Instance.getTxTimeoutInMillis());     //avoid the 0 ms problem
                    if(leffTestLog)logger.debug("TESTING - I woke myself up :C , I slept for " + DBMSImpl.Instance.getTxTimeoutInMillis() + "  mill seconds");
                    break;                                                      //if I woke myself up, rollback
                }catch (InterruptedException e){                                //otherwise try again
                    if(leffTestLog)logger.debug("I was woken up!");
                }
                if(tripleTryCheck)logger.debug("TESTING - Trying again");
            }
        }
        if (!gotLock){                                          //if I woke myself up, rollback
            rollback();
            throw new ClientTxRolledBackException("Failed to get the lock");
        }

        boolean retIJustGotTheLock = iJustGotTheLock.get();
        iJustGotTheLock.set(false); //reset it
        if (verbose) logger.debug("iJustGotTheLock = " + retIJustGotTheLock);
        if (verbose)
            logger.debug("The lock should be reset to false, iJustGotTheLock =  " + iJustGotTheLock.get() + "\n");
        if(leffTestLog)logger.debug("TESTING - I finished getting the lock for table = "+tableName+", key = "+key);
        return retIJustGotTheLock;
    }

    void clearTxsLocks()
    {
        UnlockAllResources();
        wakeUpWaitingList(Thread.currentThread());//<-- v0c
    }

    private void rollback() throws SystemException
    {
        if(leffTestLog)logger.debug("TESTING - calling rollback from the lockmanager");
        if(vTest)logger.debug("RollingBack");
        TxMgrImpl.Instance.rollback();
    }

    /**
     * Attempt to lock a resource. If the resource is unlocked lock it.
     * If the resource is locked, do nothing
     *
     * @return if the resource is unlocked or locked by this thread, return this thread
     */
    private <K> Thread tryToLockResource(String tableName, K key)
    {
        //TODO - If the resource is a createTable the key will be null
        Thread lockingThread = Thread.currentThread();
        ConcurrentHashMap<K, Thread> tkts;
        boolean isLocked = true;
        synchronized (this) {
            tkts = (ConcurrentHashMap<K, Thread>) globalLocks.get(tableName);   //check if something in this table is locked
            if (!(tkts == null)) {                                              //if something in the table is locked
                Thread currentLockHolder = tkts.get(key);                       //get the thread that is locking the resource "key"
                if (currentLockHolder == null)
                    isLocked = false;                //if this map entry is unlocked, lock it (see below)
                else if (currentLockHolder.equals(Thread.currentThread()))      //if this thread already holds the lock
                    isLocked = true;                                            //then no need to update it
                else if (!currentLockHolder.equals(Thread.currentThread()))     //if this thread doesn't hold the lock
                    lockingThread = currentLockHolder;                          //figure out who does
            } else                                                              //if nothing in this table is locked
                isLocked = false;                                               //lock it (see below)
            if (!isLocked) {                                                    //Lock this thread
                lockResource(tableName, key, Thread.currentThread());
                iJustGotTheLock.set(true);
            }
        }
        return lockingThread;
    }

    /**
     * Lock a resource.
     * TODO - If the resource is a createTable the key will be null
     * <p>
     * If the resource is a mapEntry
     * Lock the mapEntry in table "tableName" that has the key "key"
     * <p>
     * This method assumes it's unlocked
     */
    private <K> void lockResource(String tableName, K key, Thread lockingThread)
    {
        //TODO - If the resource is a createTable the key will be null

        //  Add it to the global list of locks   //
        ConcurrentHashMap<K, Thread> tempMap;
        if (globalLocks.containsKey(tableName)) {                             //if this table is active
            tempMap = (ConcurrentHashMap<K, Thread>) globalLocks.get(tableName);
        } else {                                                              //if this table is inactive
            tempMap = new ConcurrentHashMap<>();
            globalLocks.put(tableName, tempMap);
        }
        tempMap.put(key, lockingThread);                                    //map the key and thread together
        //  Add it to the local list of locks   //
        ConcurrentHashMap<String, Set<?>> locks = localLocks.get();        //setup
        Set<K> tempSet;
        if (locks.get(tableName) == null) tempSet = Collections.synchronizedSet(new HashSet<K>());     //setup
        else tempSet = (Set<K>) locks.get(tableName);

        if (tempSet == null) {
            logger.debug("tempSet " + tempSet + "  is null");
            logger.debug("locks.isEmpty() = " + locks.isEmpty() + "  ");
            logger.debug("locks = " + locks + "  ");
        }

        tempSet.add(key);                                                    //setup
        locks.put(tableName, tempSet);                                       //mark down that this thread has the lock
        if (verbose)
            logger.debug(" before localLocks = " + localLocks.get() + " ehehehehehehehehehehehehehehehehehehehehehehehhkjfhekhfjehfkjhfkjshfehk");
    }

    //Why would I ever want to do this?
//    synchronized private <K> void UnlockResourcesInTable(String tableName)
//    {
//        //TODO - If the resource is a createTable the key will be null
//        if(tableName == null) throw new InvalidParameterException("UnlockResourcesInTable: tableName cannot be null");
//        if(!globalLocks.containsKey(tableName))
//            throw new InvalidParameterException("UnlockResourcesInTable: table" + tableName+ " doesn't have any locked resources");
//        globalLocks.remove(tableName);
//    }

    /**
     * Unlock all resources that this thread has locked
     */
    synchronized private void UnlockAllResources()
    {
        //TODO - If the resource is a createTable the key will be null
        ConcurrentHashMap<String, Set<?>> thisThreadsLocks = localLocks.get();              //get the group of local locks
        if (!(thisThreadsLocks == null)) {                                                   //if it's null then it has nothing to clear
            for (String tableName : thisThreadsLocks.keySet()) {                            //iterate through the tables this thread has locked stuff in
                Set<?> lockedResources = thisThreadsLocks.get(tableName);                   //get all the locked keys of this table
                ConcurrentHashMap<?, Thread> lockedMapEntries = globalLocks.get(tableName); //get the global list of locked keys for this table
                for (Object o : lockedResources)
                    lockedMapEntries.remove(o);             //for each local lock, unlock it (from the global list)
            }
        }
        localLocks.set(new ConcurrentHashMap<>());                                          //reset the localLocks
    }

    /**
     * This is called by a waitingThread who attempted to access a
     * resource that lockingThread has the lock for.
     * This adds waitingThread to lockingThread's waitinglist so that
     * when lockingThread commits or rollsback, it will send a wakeup
     * call to the calling thread
     *
     * @param lockingThread
     * @param waitingThread
     */
    private void addToWaitList(Thread lockingThread, Thread waitingThread)
    {
        //if the locking thread already has a waiting list
        if (waitingLists.containsValue(lockingThread)) {                  //if this thread already has a wait list
            Set<Thread> waitingList = waitingLists.get(lockingThread);  //get the group of threads on the waitlist
            waitingList.add(waitingThread);                             //add waitingThread to the list
        } else {
            Set<Thread> waitingList = Collections.synchronizedSet(new HashSet<Thread>());
            waitingList.add(waitingThread);
            waitingLists.put(lockingThread, waitingList);
        }
    }

    /**
     * If this thread has a waiting list, wake up everyone on that list
     *
     * @param lockThread
     */
    synchronized private void wakeUpWaitingList(Thread lockThread)
    {
        if(leffTestLog) logger.debug("TESTING - waking up threads");
        if (lockThread == null) throw new InvalidParameterException();
        if (waitingLists.containsKey(lockThread)) {                      //if this thread has a waitinglist
            Set<Thread> waitingList = waitingLists.get(lockThread);      //get the waitinglist
            for (Thread asleep : waitingList) {
                      //wake up all waiting threads
                    if(leffTestLog) logger.debug("TESTING - about to wake up thread "+asleep.toString());
                    asleep.interrupt();
            }
        }
        waitingLists.remove(lockThread);                                //remove the thread's waitingList
    }

}
