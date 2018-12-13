package edu.yu.oats.oatsdb.dbms.v0;

import edu.yu.oats.oatsdb.dbms.ClientNotInTxException;
import edu.yu.oats.oatsdb.dbms.SystemException;
import edu.yu.oats.oatsdb.dbms.TxStatus;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Got the basics of this code from:
 * https://www.logicbig.com/tutorials/core-java-tutorial/java-dynamic-proxies/method-interceptors.html
 * @param <T>
 */
public class DbmsTxOnlyMap<T> implements InvocationHandler
{
    private T t;

    public DbmsTxOnlyMap(T t) {
        this.t = t;
    }

    //@Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        //TODO - replace the printlns with logs
        //System.out.println("before method call : " + method.getName());
        confirmClientIsInATx();
        //System.out.println("Confirmed - it's in a transaction");
        Object result = method.invoke(t, args);
        //System.out.println("after method call : " + method.getName());
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getProxy(T t, Class<? super T> interfaceType) {
        DbmsTxOnlyMap handler = new DbmsTxOnlyMap(t);
        return (T) Proxy.newProxyInstance(interfaceType.getClassLoader(),
                new Class<?>[]{interfaceType}, handler
        );
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
