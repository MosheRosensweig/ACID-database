package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NamedConcurrentHashMap<K,V> implements Map<K,V>, Serializable
{
    //////////////////////
    //  Logger Stuff    //
    //////////////////////
    private static Logger logger = LogManager.getLogger();
    private final static boolean verbose = false; // used for controlling the logger
    private final static boolean leffTestLog = true; // used for controlling the logger for Leff tests
    /////////////////////////////
    //  End of Logger Stuff    //
    /////////////////////////////

    private final ConcurrentHashMap<K,V> theMap = new ConcurrentHashMap<>();
    private final String name;
    private Tx transaction;
    private final HashSet<K> keysToDelete = new HashSet<>();       //set of things to delete from the realDB
    //TODO - rethrow the exceptions


    public NamedConcurrentHashMap(String name, Tx transaction)
    {
        super();
        this.name = name;
        this.transaction = transaction;
        if(leffTestLog)logger.debug("TESTING - making a new ShadowDB Table named "+name);
    }

    public String getName()
    {
        confirmClientIsInATx();
        return name;
    }

    @Override
    public V put(K key, V value)
    {
        if(leffTestLog)logger.debug("TESTING - starting a put on table "+name+", key = "+key+", value = "+value);
        confirmClientIsInATx();
        try {
            LockManager.Instance.getResourceLock(name, key);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        }
        //if this key was on the list of things to delete from the realDB, now it shouldn't be on that list
        keysToDelete.remove( (K)key);
        if(leffTestLog)logger.debug("TESTING - ending a put on table "+name+", key = "+key+", value = "+value);
        return theMap.put(key, value);
    }

    @Override
    public V remove(Object key)
    {
        if(leffTestLog)logger.debug("TESTING - starting a remove on table "+name+", key = "+key);
        confirmClientIsInATx();
        try {
            LockManager.Instance.getResourceLock(name, key);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        }
        keysToDelete.add((K) key);
        if(leffTestLog)logger.debug("TESTING - starting a put on table "+name+", key = "+key);
        return theMap.remove(key);
    }

    @Override
    public V get(Object key)
    {
        if(leffTestLog)logger.debug("TESTING - starting a get on table "+name+", key = "+key);
        confirmClientIsInATx();
        if(verbose)logger.debug("Get " + key + " from table : "+name);
        boolean iJustGotTheLock = false;
        try {
            iJustGotTheLock = LockManager.Instance.getResourceLock(name, key);
            if(verbose)logger.debug("iJustGotTheLock = "+iJustGotTheLock);
            //TODO - get the actual resource

            //if I just got the lock, update the map
            //if I just got the lock, then this is the first time I'm importing the value into the shadowDB
            if(iJustGotTheLock) DBMSImpl.Instance.updateMapValue(name, key);
            if(leffTestLog && iJustGotTheLock)logger.debug("TESTING - I just got lock for a get on table "+name+", key = "+key);
            else if (leffTestLog &&(!iJustGotTheLock))logger.debug("TESTING - I did NOT just get the lock for a get on table "+name+", key = "+key);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SystemException e) {
            e.printStackTrace();
        }
        if(leffTestLog)logger.debug("TESTING - ending a get on table "+name+", key = "+key);
        return theMap.get(key);
    }

    private void confirmClientIsInATx()
    {
//        if(transaction.getCompletionStatus() != TxCompletionStatus.NOT_COMPLETED)
//            throw new ClientNotInTxException("This is from a transaction that has already completed");
        TxStatus txStatus = TxStatus.UNKNOWN;
        try {
            txStatus = TxMgrImpl.Instance.getStatus();
        } catch (SystemException e) {
            //TODO - implement this
            e.printStackTrace();
        }
        if(txStatus.equals(TxStatus.NO_TRANSACTION)) throw new ClientNotInTxException("");
    }

    @Override
    public boolean equals(Object o)
    {
        confirmClientIsInATx();
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedConcurrentHashMap<?, ?> that = (NamedConcurrentHashMap<?, ?>) o;
        return Objects.equals(theMap, that.theMap) &&
                Objects.equals(name, that.name) &&
                Objects.equals(transaction, that.transaction);
    }

    @Override
    public int hashCode()
    {
        confirmClientIsInATx();
        return Objects.hash(theMap, name, transaction);
    }

    HashSet<K> getKeysToDelete()
    {
        return keysToDelete;
    }
    NamedConcurrentHashMap<K,V> getThis()
    {
        return this;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        confirmClientIsInATx();
        theMap.putAll(m);
    }

    @Override
    public void clear()
    {
        confirmClientIsInATx();
        theMap.clear();
    }

    @Override
    public Set<K> keySet()
    {
        confirmClientIsInATx();
        return theMap.keySet();
    }

    @Override
    public Collection<V> values()
    {
        confirmClientIsInATx();
        return theMap.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        confirmClientIsInATx();
        return theMap.entrySet();
    }

    @Override
    public int size()
    {
        confirmClientIsInATx();
        return theMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        confirmClientIsInATx();
        return theMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        confirmClientIsInATx();
        return theMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        confirmClientIsInATx();
        return theMap.containsValue(value);
    }
}
