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
import alluxio.exception.status.UnauthenticatedException;
import alluxio.network.thrift.ThriftUtils;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

public class GssapiTransportProvider implements TransportProvider {
  private final String serviceName = Configuration.get(PropertyKey.SECURITY_SASL_SERVICE_NAME);
  private final Map<String, String> props = new HashMap<String, String>(2);
  public static String MECHANISM = "GSSAPI";

  public GssapiTransportProvider() {
    props.put(Sasl.QOP, "auth");
    props.put(Sasl.SERVER_AUTH, "true");
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    // Do not use it in kerberized cluster.
    return null;
  }

  @Override
  public TTransport getClientTransport(Subject subject, InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    final TTransport transport = ThriftUtils.createThriftSocket(serverAddress);
    Exception exception = new Exception();
    try {
      return Subject.doAs(subject, new PrivilegedExceptionAction<TTransport>() {
        @Override
        public TTransport run() throws SaslException {
          return new TSaslClientTransport(
            MECHANISM,
            null,
            serviceName,
            serverAddress.getHostName(),
            props,
            new EmptyClientCallbackHandler(),
            transport);
        }
      });
    } catch (Exception e) {
      exception = e;
    }
    throw new UnauthenticatedException(exception.getMessage(), exception);
  }

  @Override
  public TTransportFactory getServerTransportFactory(String serverName)
      throws SaslException {
    TSaslServerTransport.Factory serverFactory = new TSaslServerTransport.Factory();
    serverFactory.addServerDefinition(
      MECHANISM,
      serviceName,
      serverName,
      props,
      new GssKrbServerCallbackHandler());
    return serverFactory;
  }

  @Override
  public TTransportFactory getServerTransportFactory(Runnable runnable, String serverName)
      throws SaslException {
    // Do not use it in kerberized cluster.
    return null;
  }
}
