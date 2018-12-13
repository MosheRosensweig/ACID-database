package edu.yu.oats.oatsdb.dbms.v0c;

import edu.yu.oats.oatsdb.dbms.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Each <b>table</b> in our database is represented by a <b>map</b>.
 *
 * @author mosherosensweig
 * @version 10/21/18 AM
 */
public enum DBMSImpl implements ConfigurableDBMS
{
    Instance;
    //////////////////////
    //  Logger Stuff    //
    //////////////////////
    private static Logger logger = LogManager.getLogger();
    private boolean leffTestLog = false;
    /////////////////////////////
    //  End of Logger Stuff    //
    /////////////////////////////

    //database holds all our tables, so it is a map of maps.
    private static final Map<String, Map<?, ?>> database = new ConcurrentHashMap<String, Map<?, ?>>();
    private static final Map<String, Class<?>> database_names_to_key_types = new ConcurrentHashMap<String, Class<?>>();
    private static final Map<String, Class<?>> database_names_to_value_types = new ConcurrentHashMap<String, Class<?>>();
    private int txTimeoutLimit = 5000;
    //ShadowDB
    private ThreadLocal<Map<String, Map<?, ?>>> shadowDB = ThreadLocal.withInitial(ConcurrentHashMap::new);
    private ThreadLocal<Map<String, Class<?>>> shadowDB_names_to_key_types =ThreadLocal.withInitial(ConcurrentHashMap::new);
    private ThreadLocal<Map<String, Class<?>>> shadowDB_names_to_value_types = ThreadLocal.withInitial(ConcurrentHashMap::new);
    ThreadLocal<Tx> tx = new ThreadLocal<>();//can't initialize it here because of the stupid exceptions //intentionally not private

    /**
     * Retrieves the named map.  The map is "typed" by its key and value classes
     * and this API provides a type-safe way for the map retrieval.
     *
     * @param s       name associated with the map, cannot be empty
     * @param aClass  the type of the map's keys, cannot be null
     * @param aClass1 the type of the map's values, cannot be null
     * @return the named map
     * @throws ClassCastException     if specified key or value class doesn't match
     *                                the corresponding classes of the actual map
     * @throws NoSuchElementException if no map is associated with the specified
     *                                name
     * @throws ClientNotInTxException if client is not associated with a
     *                                transaction.
     * @see #createMap
     */
    public <K, V> Map<K, V> getMap(String s, Class<K> aClass, Class<V> aClass1)
    {
        if(leffTestLog) logger.debug("TESTING - GetMap name = " +s);
        //1] Confirm it's in a transaction
        confirmClientIsInATx();
        //2] Validate the input
        checkForNullInput(s, aClass, aClass1);

        //check which db it's in - if it's in Shadow DB, use that one
        boolean inShadowDB = false;
        boolean inDatabase = false;
        if (shadowDB.get().keySet().contains(s)) inShadowDB = true;
        else if (database.keySet().contains(s)) inDatabase = true;
        if (!inDatabase && !inShadowDB) throw new NoSuchElementException("Table " + s + " doesn't exist");
        //3] Check type safety
        //I need to do this in two separate blocks of code, because the shadowDB might be using a table that doesn't
        //yet exist in the real db
        if (inShadowDB) {
            if (!shadowDB_names_to_key_types.get().get(s).equals(aClass))
                throw new ClassCastException("The parameterized key type doesn't match the actual key type");
            if (!shadowDB_names_to_value_types.get().get(s).equals(aClass1))
                throw new ClassCastException("The parameterized value type doesn't match the actual value type");
        } else { // if (inDatabase) {
            if (!database_names_to_key_types.get(s).equals(aClass))
                throw new ClassCastException("The parameterized key type doesn't match the actual key type");
            if (!database_names_to_value_types.get(s).equals(aClass1))
                throw new ClassCastException("The parameterized value type doesn't match the actual value type");
        }
        //4] initialize the shadowDB
        if(shadowDB.get() == null) initialize();
        if(tx.get() == null) initializeTx();
        //5]
        Map<K, V> retrievedTable;
        if (inShadowDB) retrievedTable = (Map<K, V>) shadowDB.get().get(s);
        else {
            Map<K, V> newTable;
            newTable = new NamedConcurrentHashMap<K, V>(s, tx.get());
            addToShadowDB(newTable, s, aClass, aClass1);
            retrievedTable = newTable; //make a new empty shadowMap --> based on Leff's way
        }
        if(leffTestLog) logger.debug("TESTING - END GetMap name = " +s);
        ClientMap<K,V> clientMap = new ClientMap<>(retrievedTable,tx.get(), s, aClass, aClass1);
        return clientMap;
    }

    /**
     * Creates (and returns) a map, associates it with the specified name for
     * subsequent retrieval.  The map is parameterized by the type of the key
     * class and the type of the value class
     * <p>
     * NOTE: Creating a map is equivalent to creating a new table in the Database.
     *
     * @param s       names the map (for subsequent retrieval), cannot be empty
     * @param aClass  the type of the map's keys, cannot be null
     * @param aClass1 the type of the map's values, cannot be null
     * @return a parameterized map of the specified key and value types
     * @throws IllegalArgumentException if name is already bound to another map
     * @throws ClientNotInTxException   if client is not associated with a
     *                                  transaction.
     */
    public <K, V> Map<K, V> createMap(String s, Class<K> aClass, Class<V> aClass1)
    {
        if(leffTestLog) logger.debug("TESTING - START createMap name = " +s+", aclass = "+ aClass + ",aclass1"+ aClass1);
        //1] Confirm it's in a transaction
        confirmClientIsInATx(); //throws ClientNotInTxException
        //2] Validate the input
        checkForNullInput(s, aClass, aClass1);
        //check if a table by that name already exists
        //3] initialize the shadowDB
        if(shadowDB.get() == null) initialize();
        if(tx.get() == null) initializeTx();
        if (database.keySet().contains(s) ||
                shadowDB.get().keySet().contains(s))//split for debugging purposes
            throw new IllegalArgumentException("The name " + s + " is already in use");
        //4] Add a new table to the shadow database
        Map<K, V> newTable = new NamedConcurrentHashMap<K, V>(s, tx.get());
        addToShadowDB(newTable, s, aClass, aClass1);
        if(leffTestLog) logger.debug("TESTING - END createMap name = " +s);
        ClientMap<K,V> clientMap = new ClientMap<>(newTable,tx.get(), s, aClass, aClass1);
        return clientMap;
    }

    private void checkForNullInput(String name, Class<?> keyType, Class<?> valueType)
    {
        if (name == null || name.equals(""))
            throw new InvalidParameterException("The table name must not be null or empty");
        if (keyType == null) throw new InvalidParameterException("The key type must not be null");
        if (valueType == null) throw new InvalidParameterException("The value type must not be null");
    }

    private void addToDatabase(Map<?, ?> newTable, String name, Class<?> keyType, Class<?> valueType)
    {
        database.put(name, newTable);
        database_names_to_key_types.put(name, keyType);
        database_names_to_value_types.put(name, valueType);
    }

    //TODO - uses this
    //make sure to remove the table (map) from all 3 places
    private void removeFromDatabase(String name)
    {
        database.remove(name);
        database_names_to_key_types.remove(name);
        database_names_to_value_types.remove(name);
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
        if (txStatus.equals(TxStatus.NO_TRANSACTION)) throw new ClientNotInTxException("");
    }

    //////////////////////
    //  ShadowDB Stuff  //
    //////////////////////

    private void initialize()
    {
        if(leffTestLog)logger.debug("TESTING - initializing the shadowdb");
        shadowDB.set(new ConcurrentHashMap<>());
        shadowDB_names_to_key_types.set(new ConcurrentHashMap<>());
        shadowDB_names_to_value_types.set(new ConcurrentHashMap<>());
    }
    private void initializeTx()
    {
        if(leffTestLog)logger.debug("TESTING - initializing the tx");
        try {
            tx.set(TxMgrImpl.Instance.getTx());
        } catch (SystemException e) {
            e.printStackTrace();
        }
    }

    //add a shadowTable to the shadowDB
    private void addToShadowDB(Map<?, ?> newTable, String name, Class<?> keyType, Class<?> valueType)
    {
        shadowDB.get().put(name, newTable);
        shadowDB_names_to_key_types.get().put(name, keyType);
        shadowDB_names_to_value_types.get().put(name, valueType);
    }

    //TODO - uses this
    //make sure to remove the table (map) from all 3 places
    private void removeFromShadowDB(String name)
    {
        shadowDB.get().remove(name);
        shadowDB_names_to_key_types.get().remove(name);
        shadowDB_names_to_value_types.get().remove(name);
        //TODO make sure to remove the threadlocals
    }

    // Cut ties with the shadowDB
    private void clearShadowDB()
    {
        if(leffTestLog)logger.debug("TESTING - clearing the shadowDB");
        shadowDB.set(new ConcurrentHashMap<String, Map<?, ?>>());
        shadowDB_names_to_key_types.set(new ConcurrentHashMap<String, Class<?>>());
        shadowDB_names_to_value_types.set(new ConcurrentHashMap<String, Class<?>>());
    }

    /**
     * 1] Take all the map entries in the shadowDB and ".put()" them in the actual DB
     * 2] Take all the "removes" from each map, and remove them from the database
     */
    synchronized <K, V> void commit() throws IOException, ClassNotFoundException
    {
        if(leffTestLog)logger.debug("TESTING - doing the dbms commiting part of commit");
        Map<String, Map<?, ?>> tempShadowDB = shadowDB.get();                       //get the shadowDB
        Map<String, Class<?>> tempShadowDBKeys = shadowDB_names_to_key_types.get();
        Map<String, Class<?>> tempShadowDBValues = shadowDB_names_to_value_types.get();

        //for some unexplained reason, if I serialize the entire shadowDB at once
        //I get RANDOM issues, but If I do it piece by piece it works... very weird
        //I do this 1st to make sure everything is serializable before I upload it
        for(String tableName : tempShadowDB.keySet()){
            NamedConcurrentHashMap<K, V> shadowDBTable =
                    (NamedConcurrentHashMap<K, V>) tempShadowDB.get(tableName);
            for (K key : shadowDBTable.keySet()){
                shadowDBTable.put((K) DeepCopy.deepCopy(key), (V) DeepCopy.deepCopy(shadowDBTable.get(key)));
            }
        }
        for (String tableName : tempShadowDB.keySet()) {                            //for each table in the shadowDB
            NamedConcurrentHashMap<K, V> shadowDBTable =
                    (NamedConcurrentHashMap<K, V>) tempShadowDB.get(tableName);     //get the shadowDB table with that name
            Map<K, V> realDBTable = (Map<K, V>) database.get(tableName);            //get the realDB table with that name
            if (realDBTable == null) {                                              //if this table doesn't exist yet in the real db
                realDBTable = new ConcurrentHashMap<>();                            //create a new table of type ConcurrentHashMap
                database_names_to_key_types.put(tableName, tempShadowDBKeys.get(tableName));
                database_names_to_value_types.put(tableName, tempShadowDBValues.get(tableName));
                database.put(tableName, realDBTable);
            }
            //  Now that the realDB has an instance of a table with the name "tableName"
            for (K key : shadowDBTable.keySet())                                    //add the new values to the realDB
                realDBTable.put(key,shadowDBTable.get(key)); //<-- I deepcopied the entire shadowDb
            for (K delete : shadowDBTable.getKeysToDelete()) realDBTable.remove(delete);    //delete anything that should be deleted

        }
        clearShadowDB();                                                            //reset the shadowDB
    }

    /**
     * reset the shadowDB
     */
    void rollback()
    {
        clearShadowDB();
    }

    /**
     * When a map calls .get() and acquires the lock for the 1st time
     * Update that map entry into the shadowDB based on the real DB
     *
     * @param tableName
     * @param key
     * @param <K>
     * @param <V>
     */
    <K, V> void updateMapValue(String tableName, K key)
    {
        if(leffTestLog)logger.debug("TESTING - bringing in a new value from the realDB to the shadowDB, tableName = "
                +tableName+", key = "+key);
        Map<K, V> table = (Map<K, V>) database.get(tableName);
        if (table == null)
            throw new IllegalStateException("realMapDotGet : The table " + tableName + " doesn't exist but should");                               //if this value doesn't yet exist in the realDB
        Class<V> valueClass = (Class<V>) database_names_to_key_types.get(tableName);
        V value = null;
        try {
            value = (V) DeepCopy.deepCopy(table.get(key));
        } catch (IOException e) { //If it got into the database, that means that it's serializable
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Map<K, V> shadDB = (Map<K, V>) shadowDB.get().get(tableName);
        //if the value is null, then there is nothing to add, and any calls to the shadowDB should return null anyways
        if(value != null)shadDB.put(key, value);
    }

    //////////////////////
    //  Timeout Stuff   //
    //////////////////////

    /**
     * Sets the duration of the "transaction timeout".  A client whose
     * transaction's duration exceeds the DBMS's timeout will be automatically
     * rolled back by the DBMS.
     *
     * @param ms the timeout duration in ms, must be greater than 0
     */
    public void setTxTimeoutInMillis(int ms)
    {
        if(leffTestLog)logger.debug("TESTING - the timeout is set to "+ms+" ms");
        if (ms <= 0) throw new InvalidParameterException("Ms cannot be less than or equal to 0");
        txTimeoutLimit = ms;
    }

    /**
     * Returns the current DBMS transaction timeout duration.
     *
     * @return duration in milliseconds, if it was uninitialized, return 5000
     */
    public int getTxTimeoutInMillis()
    {
        return txTimeoutLimit;
    }
}
