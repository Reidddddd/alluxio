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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;

public class GssKrbServerCallbackHandler implements CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(GssKrbServerCallbackHandler.class);

  @Override
  public void handle(Callback[] callbacks)
      throws IOException, UnsupportedCallbackException {
    for (Callback cb : callbacks) {
      if (cb instanceof AuthorizeCallback) {
        AuthorizeCallback acb = (AuthorizeCallback) cb;
        if (acb.getAuthenticationID().equals(acb.getAuthorizationID())) {
          acb.setAuthorized(true);
          LOG.info("{} is authorized", acb.getAuthenticationID());
          KerberosName name = KerberosName.parsePrincipalName(acb.getAuthenticationID());
          AuthenticatedClientUser.set(name.getName());
        }
      }
    }
  }
}
