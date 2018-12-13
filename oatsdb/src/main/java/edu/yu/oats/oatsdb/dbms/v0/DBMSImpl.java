package edu.yu.oats.oatsdb.dbms.v0;
import edu.yu.oats.oatsdb.dbms.ClientNotInTxException;
import edu.yu.oats.oatsdb.dbms.DBMS;
import edu.yu.oats.oatsdb.dbms.SystemException;
import edu.yu.oats.oatsdb.dbms.TxStatus;

import java.security.InvalidParameterException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Each <b>table</b> in our database is represented by a <b>map</b>.
 *
 *
 * @author mosherosensweig
 * @version 9/28/16 AM
 */
public enum DBMSImpl implements DBMS
{
    Instance;
    //DATABASE holds all our tables, so it is a map of maps.

    //TODO - remove this old code
//    private static final DBMap<String, Map<?,?>> DATABASE = new DBMap<String, Map<?, ?>>();
//    private static final DBMap<String, Class<?>> DATABASE_NAMES_TO_KEY_TYPES = new DBMap<String, Class<?>>();
//    private static final DBMap<String, Class<?>> DATABASE_NAMES_TO_VALUE_TYPES = new DBMap<String, Class<?>>();

     private static final Map<String, Map<?,?>> DATABASE = DbmsTxOnlyMap.getProxy(new ConcurrentHashMap<String, Map<?, ?>>(), Map.class);
     private static final Map<String, Class<?>> DATABASE_NAMES_TO_KEY_TYPES = DbmsTxOnlyMap.getProxy(new ConcurrentHashMap<String, Class<?>>(), Map.class);
     private static final Map<String, Class<?>> DATABASE_NAMES_TO_VALUE_TYPES = DbmsTxOnlyMap.getProxy(new ConcurrentHashMap<String, Class<?>>(), Map.class);

    /** Retrieves the named map.  The map is "typed" by its key and value classes
     * and this API provides a type-safe way for the map retrieval.
     *
     * @param name name associated with the map, cannot be empty
     * @return the named map
     * @param keyClass the type of the map's keys, cannot be null
     * @param valueClass the type of the map's values, cannot be null
     * @see #createMap
     * @throws ClassCastException if specified key or value class doesn't match
     * the corresponding classes of the actual map
     * @throws NoSuchElementException if no map is associated with the specified
     * name
     * @throws ClientNotInTxException if client is not associated with a
     * transaction.
     */
    public <K, V> Map<K, V> getMap(String s, Class<K> aClass, Class<V> aClass1)
    {
        //1] Confirm it's in a transaction
        confirmClientIsInATx();
        //2] Validate the input
        checkForNullInput(s, aClass, aClass1);
        if(!DATABASE.keySet().contains(s)) throw new NoSuchElementException("Table "+s+" doesn't exist");
        //3] Check type safety
        if(!DATABASE_NAMES_TO_KEY_TYPES.get(s).equals(aClass))
            throw new ClassCastException("The parameterized key type doesn't match the actual key type");
        if(!DATABASE_NAMES_TO_VALUE_TYPES.get(s).equals(aClass1))
            throw new ClassCastException("The parameterized value type doesn't match the actual value type");

        Map<K, V> retrievedTable = (Map<K, V>) DATABASE.get(s);
        return retrievedTable;
    }

    /** Creates (and returns) a map, associates it with the specified name for
     * subsequent retrieval.  The map is parameterized by the type of the key
     * class and the type of the value class
     *
     * NOTE: Creating a map is equivalent to creating a new table in the Database.
     *
     * @param name names the map (for subsequent retrieval), cannot be empty
     * @param keyClass the type of the map's keys, cannot be null
     * @param valueClass the type of the map's values, cannot be null
     * @return a parameterized map of the specified key and value types
     * @throws IllegalArgumentException if name is already bound to another map
     * @throws ClientNotInTxException if client is not associated with a
     * transaction.
     */
    public <K, V> Map<K, V> createMap(String s, Class<K> aClass, Class<V> aClass1)
    {
        //1] Confirm it's in a transaction
        confirmClientIsInATx(); //throws ClientNotInTxException
        //2] Validate the input
        checkForNullInput(s, aClass, aClass1);
        //check if a table by that name already exists
        if(DATABASE.keySet().contains(s)) throw new IllegalArgumentException("The name " + s + " is already in use");

        //3] Add a new table to the database
// Map<K, V> newTable = new DBMap<K, V>(); //TODO - remove this old code
        Map<K, V> newTable = DbmsTxOnlyMap.getProxy(new ConcurrentHashMap<K, V>(), Map.class);
        addToDatabase(newTable, s, aClass, aClass1);
        return newTable;
    }

    private void checkForNullInput(String name, Class<?> keyType, Class<?> valueType)
    {
        if(name == null || name.equals("")) throw new InvalidParameterException("The table name must not be null or empty");
        if(keyType == null) throw new InvalidParameterException("The key type must not be null");
        if(valueType == null) throw new InvalidParameterException("The value type must not be null");
    }

    private void addToDatabase(Map<?, ?> newTable, String name, Class<?> keyType, Class<?> valueType)
    {
        DATABASE.put(name, newTable);
        //TODO - actually make this typesafe
        DATABASE_NAMES_TO_KEY_TYPES.put(name, keyType);
        DATABASE_NAMES_TO_VALUE_TYPES.put(name, valueType);
    }

    //TODO - uses this
    //make sure to remove the table (map) from all 3 places
    private void removeFromDatabase(String name)
    {
        DATABASE.remove(name);
        DATABASE_NAMES_TO_KEY_TYPES.remove(name);
        DATABASE_NAMES_TO_VALUE_TYPES.remove(name);
        //TODO make sure to remove the threadlocals
    }

    /**
     * @throws ClientNotInTxException
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
        if(txStatus.equals(TxStatus.NO_TRANSACTION)) throw new ClientNotInTxException("");
    }
}
