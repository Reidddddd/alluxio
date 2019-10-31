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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Kerberos authentication, but it doesn't support login using ticket cache yet.
 */
public class KrbLogin {
  private static final Logger LOG = LoggerFactory.getLogger(KrbLogin.class);

  private Subject subject;
  private LoginContext loginContext;
  private CallbackHandler callbackHandler;

  public KrbLogin(final String loginContextName, CallbackHandler callbackHandler)
      throws LoginException {
    this.callbackHandler = callbackHandler;
    this.loginContext = login(loginContextName);
    this.subject = loginContext.getSubject();
    boolean isKrbTicket = !subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
    boolean isKeytabCred = !subject.getPrivateCredentials(KeyTab.class).isEmpty();
    LOG.info("isKrbTicket: {}, isKeytabCred: {}", isKrbTicket, isKeytabCred);
  }

  private synchronized LoginContext login(final String loginContextName) throws LoginException {
    if (loginContextName == null) {
      throw new LoginException("Can't find login context name, please check your JAAS file.");
    }
    LOG.info("Using {} as login context.", loginContextName);
    LoginContext context = new LoginContext(loginContextName, callbackHandler);
    context.login();
    LOG.info("{} successfully logged in.", loginContextName);
    return context;
  }

  public Subject getSubject() {
    return subject;
  }
}
