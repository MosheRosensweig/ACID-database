package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This is my proxy. Whenever the client requests a map, a proxy is generated and provided to them.
 * They can continue to use that proxy in subsequent transactions, as it will find the new shadowTable.
 *
 * This proxy stores all the relevant data only about the table it represents, i.e. the table name, the
 * key class, and the value class. It can use that data to retrieve an up-to-date map.
 * @param <K>
 * @param <V>
 */
public class ClientMap<K,V> implements Map<K, V>
{
    //////////////////////
    //  Logger Stuff    //
    //////////////////////
    private static Logger logger = LogManager.getLogger();
    private boolean verbose = true;

    private ThreadLocal<Map<K,V>> shadowMap = new ThreadLocal<>();
    private ThreadLocal<Tx> tx = new ThreadLocal<>();
    private final String tableName;
    private final Class keyClass;
    private final Class valClass;

    public ClientMap(Map<K,V> shadowTable, Tx transaction, String name, Class kClass, Class vClass)
    {
        shadowMap.set(shadowTable);
        tx.set(transaction);
        tableName = name;
        keyClass = kClass;
        valClass = vClass;
    }

    /**
     * Before any method can be called, make sure that the shadowTable
     * referenced here is up to date
     * 1] Update the tx so later checks will know that its shadowTable is up to date
     *    - If the tx is out of date, get the new one
     * 2] If the tx was out of date, that means the shadowTable was as well
     *    - Generate and store a reference to the shadowTable
     */
    private void init()
    {
        confirmClientIsInATx();                     //only make this update if the client is in a tx
        if(tx.get() == null) {                      //if this proxy is being used in a tx other than the one that created it
            try {
                tx.set(TxMgrImpl.Instance.getTx()); //setup its tx
                shadowMap.set(DBMSImpl.Instance.getMap(tableName, keyClass, valClass)); //setup its shadowMap
            } catch (SystemException e) {
                e.printStackTrace();
            }
            return;
        }
        Tx check = tx.get();                        //get the status
        TxCompletionStatus cStat = check.getCompletionStatus();
        if(cStat == TxCompletionStatus.ROLLEDBACK || cStat == TxCompletionStatus.COMMITTED){
            if(verbose)logger.debug("TESTING - cStat = " +cStat.toString());
            if(verbose) logger.debug("TESTING - The old ShadowMap had a hashcode of "+shadowMap.get().hashCode());
            try {
                tx.set(TxMgrImpl.Instance.getTx()); //update the tx
            } catch (SystemException e) {
                e.printStackTrace();
            }
            shadowMap.set(DBMSImpl.Instance.getMap(tableName, keyClass, valClass));
            if(verbose)logger.debug("TESTING - I brought in the new shadowMap with a hash of "+shadowMap.get().hashCode());
        }
    }

    /**
     * Note: This checks the state of the current thread if it's in a tx or not
     */
    private void confirmClientIsInATx()
    {
        TxStatus txStatus = TxStatus.NO_TRANSACTION;
        try {
            txStatus = TxMgrImpl.Instance.getStatus();
        } catch (SystemException e) {
            //TODO - implement this
            //e.printStackTrace();
        }
        if (txStatus.equals(TxStatus.NO_TRANSACTION)) throw new ClientNotInTxException("");
    }

    @Override
    public int size()
    {
        init();
        return shadowMap.get().size();
    }

    @Override
    public boolean isEmpty()
    {
        init();
        return shadowMap.get().isEmpty();
    }

    @Override
    public boolean containsKey(Object key)
    {
        init();
        return shadowMap.get().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value)
    {
        init();
        return shadowMap.get().containsValue(value);
    }

    @Override
    public V get(Object key)
    {
        init();
        return shadowMap.get().get(key);
    }

    @Override
    public V put(K key, V value)
    {
        init();
        return shadowMap.get().put(key, value);
    }

    @Override
    public V remove(Object key)
    {
        init();
        return shadowMap.get().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m)
    {
        init();
        shadowMap.get().putAll(m);
    }

    @Override
    public void clear()
    {
        init();
        shadowMap.get().clear();
    }

    @Override
    public Set<K> keySet()
    {
        init();
        return shadowMap.get().keySet();
    }

    @Override
    public Collection<V> values()
    {
        init();
        return shadowMap.get().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        init();
        return shadowMap.get().entrySet();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMap<?, ?> clientMap = (ClientMap<?, ?>) o;
        return Objects.equals(tableName, clientMap.tableName) &&
                Objects.equals(keyClass, clientMap.keyClass) &&
                Objects.equals(valClass, clientMap.valClass);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, keyClass, valClass);
    }
}
