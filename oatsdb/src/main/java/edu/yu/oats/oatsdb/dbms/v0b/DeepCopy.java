package edu.yu.oats.oatsdb.dbms.v0b;

import edu.yu.oats.oatsdb.dbms.NotSupportedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

/**
 * I got most of this code from https://stackoverflow.com/questions/64036/how-do-you-make-a-deep-copy-of-an-object-in-java
 * 10/11/18
 */
public class DeepCopy
{
    private final static boolean verbose = false; // used for controlling the logger
    private static Logger logger = LogManager.getLogger();

    public static byte[] convertToBytes(Object object) throws IOException
    {
        byte[] byteData = null;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            oos.flush();
            oos.close();
            bos.close();
            byteData = bos.toByteArray();
        return byteData;
    }

    public static Object convertToObject(byte[] byteData) throws IOException, ClassNotFoundException
    {
        Object object = null;
        ByteArrayInputStream bais = new ByteArrayInputStream(byteData);

            object = (Object) new ObjectInputStream(bais).readObject();
        return object;
    }

    public static Object deepCopy(Object object) throws IOException, ClassNotFoundException
    {
        if(verbose)logger.debug("Making a deep copy of object "+object);
        return convertToObject(convertToBytes(object));
    }

    public DeepCopy() throws NotSupportedException
    {
        throw new NotSupportedException("You cannot instantiate DeepCopy");
    }
}
