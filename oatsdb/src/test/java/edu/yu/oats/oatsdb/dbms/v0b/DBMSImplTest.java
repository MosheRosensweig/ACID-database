package edu.yu.oats.oatsdb.dbms.v0b;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DBMSImplTest
{

    private static Logger logger = LogManager.getLogger();

    static DBMSImpl database = DBMSImpl.Instance;
    static Map<String, Integer> teachers_salaryTable;
    static Map<String[], String> faculty_schoolTable;

    static String[] yu = {"Rabbi H", "Rabbi M"};
    static String[] yale = {"Adam", "Sam"};
    static String[] barnard = {"Sarah", "Rachel"};
    static String[] columbia ={"Berliner", "Stahler", "Fishman"};

    @BeforeClass
    public static void setUp() throws Exception
    {
        logger.debug("hey man");
        //setup teachers_salaryTable
        TxMgrImpl.Instance.begin();
        teachers_salaryTable = database.createMap("Teachers", String.class, Integer.class);
        teachers_salaryTable.put("James", 20);
        teachers_salaryTable.put("Alex", 30);
        teachers_salaryTable.put("Jess", 103);
        teachers_salaryTable.put("Winser", 47);
        //setup faculty_schoolTable
        faculty_schoolTable = database.createMap("Schools", String[].class, String.class);
        faculty_schoolTable.put(yu, "YU");
        faculty_schoolTable.put(yale, "Yale");
        faculty_schoolTable.put(barnard, "Barnard");
        faculty_schoolTable.put(columbia, "Columbia");
    }

//    @Before
//    public void setUp2() throws Exception
//    {
//        TxMgrImpl.Instance.begin();
//    }

    @AfterClass
    public static void cleanUp() throws Exception
    {
        TxMgrImpl.Instance.commit();
    }

    //////////////////////////
    //  Create Map Tests    //
    //////////////////////////
    @Test   //This is NOT supposed to fail
    public void createdMapBasicTest()
    {
        //test teachers_salaryTable
        logger.debug("\tNow doing the contains\n\n");
        assertTrue(teachers_salaryTable.containsKey("James"));
        assertTrue(teachers_salaryTable.containsKey("Alex"));
        assertTrue(teachers_salaryTable.containsKey("Jess"));
        assertTrue(teachers_salaryTable.containsKey("Winser"));
        logger.debug("\tNow doing the gets\n\n");
        teachers_salaryTable.get("James");
        assertEquals(teachers_salaryTable.get("James"), new Integer(20));
        assertEquals(teachers_salaryTable.get("Alex"), new Integer(30));
        assertEquals(teachers_salaryTable.get("Jess"), new Integer(103));
        assertEquals(teachers_salaryTable.get("Winser"), new Integer(47));

        //test faculty_schoolTable
        assertTrue(faculty_schoolTable.containsKey(yu));
        assertTrue(faculty_schoolTable.containsKey(yale));
        assertTrue(faculty_schoolTable.containsKey(barnard));
        assertTrue(faculty_schoolTable.containsKey(columbia));

        assertEquals(faculty_schoolTable.get(yu), "YU");
        assertEquals(faculty_schoolTable.get(yale), "Yale");
        assertEquals(faculty_schoolTable.get(barnard), "Barnard");
        assertEquals(faculty_schoolTable.get(columbia), "Columbia");
    }

    @Test(expected=IllegalArgumentException.class)
    public void createMapDuplicateMapNameTest1()
    {
        Map<ArrayList, String> temp = database.createMap("Teachers", ArrayList.class, String.class);
    }
    @Test(expected=IllegalArgumentException.class)
    public void createMapDuplicateMapNameTest2()
    {
        database.createMap("Schools", ArrayList.class, String.class);
    }
    @Test
    public void createMapCaseSensitiveMapNameTest1()
    {
        database.createMap("teachers", ArrayList.class, String.class);
    }

    @Test(expected=InvalidParameterException.class)
    public void createMapInvalidMapNameTest1()
    {
        database.createMap("", ArrayList.class, String.class);
    }
    @Test(expected=InvalidParameterException.class)
    public void createMapInvalidMapNameTest2()
    {
        database.createMap(null, ArrayList.class, String.class);
    }

    @Test(expected = InvalidParameterException.class)
    public void createMapNullKeyTest1()
    {
        database.createMap("fun", null, String.class);
    }
    @Test(expected = InvalidParameterException.class)
    public void createMapNullValueTest1()
    {
        database.createMap("fun", String.class, null);
    }

    ///////////////////////
    //  Get Map Tests    //
    ///////////////////////
    @Test   //This is NOT supposed to fail
    public void getMapBasicTest()
    {
        Map<String, Integer> teacherMapGet = database.getMap("Teachers", String.class, Integer.class);
        Map<String[], String> facultyTable = database.getMap("Schools", String[].class, String.class);
    }

    @Test(expected=ClassCastException.class)
    public void getMapRuntimeBadKeyTest1()
    {
        Map<Integer, Integer> teacherMapGet3 = database.getMap("Teachers", Integer.class, Integer.class);
    }

    @Test(expected=ClassCastException.class)
    public void getMapRuntimeBadKeyTest2()
    {
        Map<String, String> facultyTable = database.getMap("Schools", String.class, String.class);
    }

    @Test(expected=ClassCastException.class)
    public void getMapRuntimeBadValueTest1()
    {
        Map<String, String> teacherMapGet3 = database.getMap("Teachers", String.class, String.class);
    }

    @Test(expected=ClassCastException.class)
    public void getMapRuntimeBadValueTest2()
    {
        Map<String[], String[]> facultyTable = database.getMap("Schools", String[].class, String[].class);
    }

    @Test(expected=ClassCastException.class)
    public void getMapRuntimeBadKeyAndValueTest()
    {
        Map<Character, Character> teacherMapGet3 = database.getMap("Teachers", Character.class, Character.class);
    }

    @Test(expected = InvalidParameterException.class)
    public void getMapInvalidNameTes1()
    {
        Map<Character, Character> teacherMapGet3 = database.getMap("", Character.class, Character.class);
    }
    @Test(expected = InvalidParameterException.class)
    public void getMapInvalidNameTes2()
    {
        Map<Character, Character> teacherMapGet3 = database.getMap(null, Character.class, Character.class);
    }

    @Test(expected = NoSuchElementException.class)
    public void getMapTableDoesntExistTest1()
    {
        Map<Character, Character> teacherMapGet3 = database.getMap("Fun", Character.class, Character.class);
    }

}