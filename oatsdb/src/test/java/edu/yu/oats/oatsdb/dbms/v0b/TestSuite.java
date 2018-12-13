package edu.yu.oats.oatsdb.dbms.v0b;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@Ignore
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DBMSImplTest.class,
        TxMgrImplTest.class,
        LockManagerTest.class,
        MultiThreadingTest.class
})

public class TestSuite
{


}
