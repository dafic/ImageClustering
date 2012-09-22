/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package jMEF;

/**
 *
 * @author phoenix
 */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class SerializableWritable
  implements Writable {

  private Serializable ser = null;

  private static Object toObject(byte[] bytes)
    throws IOException, ClassNotFoundException {

    // create input streams for creating object from byte array input stream
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);

    // read the Object from the byte array input stream created from the byte
    // array passed
    Object bObject = ois.readObject();

    // close the input streams
    bais.close();
    ois.close();

    // return the new Object value
    return bObject;
  }

  private static byte[] fromObject(Object toBytes)
    throws IOException {

    // create output streams for writing object to a byte array output stream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);

    // write the object to the byte array output stream and flush
    oos.writeObject(toBytes);
    oos.flush();

    // convert the byte array output stream to a byte array
    byte[] objBytes = baos.toByteArray();

    // close the output streams
    baos.close();
    oos.close();

    // return the byte array
    return objBytes;
  }

  public SerializableWritable() {

  }

  public SerializableWritable(Serializable ser) {
    this.ser = ser;
  }

  public void readFields(DataInput in)
    throws IOException {

    try {

      int numBytes = in.readInt();
      byte[] objBytes = new byte[numBytes];
      in.readFully(objBytes);
      Object obj = toObject(objBytes);
      ser = (Serializable)obj;
    }
    catch (ClassNotFoundException e) {
      throw new IOException("Class not found initializing object: "
        + e.getClass());
    }
  }

  public final static SerializableWritable read(DataInput in)
    throws IOException {
    
    SerializableWritable serWrite = new SerializableWritable();
    serWrite.readFields(in);
    return serWrite;
  }

  public void write(DataOutput out)
    throws IOException {

    byte[] objBytes = ((ser != null) ? fromObject(ser) : new byte[0]);
    out.writeInt(objBytes.length);
    out.write(objBytes);
  }

  public Serializable getSerializable() {
    return this.ser;
  }

  public void setSerializable(Serializable ser) {
    this.ser = ser;
  }

}
