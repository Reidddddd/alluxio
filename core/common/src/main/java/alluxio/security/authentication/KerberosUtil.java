/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.PropertyKey;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedAction;

public class KerberosUtil {
  private KerberosUtil(){}

  private static Subject jvmSubject;
  private static int securityEnable = -1;

  public static boolean isSecurityEnable() {
    if (securityEnable < 0) {
      AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      securityEnable = authType == AuthType.KERBEROS ? 0 : 1;
    }
    return securityEnable == 0;
  }

  public static synchronized void setSubject(Subject subject) {
    jvmSubject = subject;
  }

  public static synchronized Subject getJvmSubject() {
    return jvmSubject;
  }

  public static TTransport wrapTransportWithSubject(TTransport transport) {
    return !isSecurityEnable() ? transport :
      new TSubjectTransport(transport, KerberosUtil.getJvmSubject());
  }

  public static TTransportFactory wrapTransportFactoryWithSubject(TTransportFactory transFactory) {
    return !isSecurityEnable() ? transFactory :
      new TSubjectTransportFactory(transFactory, KerberosUtil.getJvmSubject());
  }

  public static String getClientName(Subject subject) {
    Object[] principals = subject.getPrincipals().toArray();
    Principal userName = (Principal) principals[0];
    KerberosName krbName = KerberosName.parsePrincipalName(userName.getName());
    return krbName.getName();
  }

  private static class TSubjectTransport extends TTransport {

    private TTransport transport;
    private Subject subject;

    TSubjectTransport(TTransport transport, Subject subject) {
      this.transport = transport;
      this.subject = subject;
    }

    @Override
    public boolean isOpen() {
      return transport.isOpen();
    }

    @Override
    public void open() throws TTransportException {
      Subject.doAs(subject, new PrivilegedAction<Void>() {
        @Override
        public Void run() {
          try {
            transport.open();
          } catch (TTransportException e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    }

    @Override
    public boolean peek() {
      return transport.peek();
    }

    @Override
    public void close() {
      transport.close();
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
      return transport.read(buf, off, len);
    }

    @Override
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
      return transport.readAll(buf, off, len);
    }

    @Override
    public void write(byte[] buf) throws TTransportException {
      transport.write(buf);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
      transport.write(buf, off, len);
    }

    @Override
    public void flush() throws TTransportException {
      transport.flush();
    }

    @Override
    public byte[] getBuffer() {
      return transport.getBuffer();
    }

    @Override
    public int getBufferPosition() {
      return transport.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
      return transport.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
      transport.consumeBuffer(len);
    }
  }

  private static class TSubjectTransportFactory extends TTransportFactory {

    private final TTransportFactory factory;
    private final Subject subject;

    TSubjectTransportFactory(TTransportFactory factory, Subject subject) {
      this.factory = factory;
      this.subject = subject;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return Subject.doAs(subject, new PrivilegedAction<TTransport>() {
        @Override
        public TTransport run() {
          return factory.getTransport(trans);
        }
      });
    }
  }
}
