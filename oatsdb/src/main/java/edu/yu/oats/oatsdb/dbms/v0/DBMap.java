package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.ClientNotInTxException;
import edu.yu.oats.oatsdb.dbms.SystemException;
import edu.yu.oats.oatsdb.dbms.TxStatus;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DBMap<K,V> implements Map<K,V>
{
    private ConcurrentHashMap<K,V> concurMap;

    public DBMap()
    {
        concurMap = new ConcurrentHashMap<K,V>();
    }

    public int size()
    {
        confirmClientIsInATx();
        return concurMap.size();
    }

    public boolean isEmpty()
    {
        confirmClientIsInATx();
        return concurMap.isEmpty();
    }

    public boolean containsKey(Object key)
    {
        confirmClientIsInATx();
        return concurMap.containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        confirmClientIsInATx();
        return concurMap.containsValue(value);
    }

    public V get(Object key)
    {
        confirmClientIsInATx();
        return  concurMap.get(key);
    }

    public V put(K key, V value)
    {
        confirmClientIsInATx();
        return concurMap.put(key, value);
    }

    public V remove(Object key)
    {
        confirmClientIsInATx();
        return concurMap.remove(key);
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        confirmClientIsInATx();
        concurMap.putAll(m);
    }

    public void clear()
    {
        confirmClientIsInATx();
        concurMap.clear();
    }

    public Set<K> keySet()
    {
        confirmClientIsInATx();
        return concurMap.keySet();
    }

    public Collection<V> values()
    {
        confirmClientIsInATx();
        return concurMap.values();
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        confirmClientIsInATx();
        return concurMap.entrySet();
    }

    private void confirmClientIsInATx()
    {
        TxStatus txStatus = TxStatus.UNKNOWN;
        try {
            txStatus = TxMgrImpl.Instance.getStatus();
        } catch (SystemException e) {
            //TODO - implement this
            e.printStackTrace();
        }
        if(!txStatus.equals(TxStatus.ACTIVE)) throw new ClientNotInTxException("");
    }
}
