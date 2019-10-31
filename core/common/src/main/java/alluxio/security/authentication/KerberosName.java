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

import java.util.UnknownFormatConversionException;

public class KerberosName {

  private final String name;
  private final String host;
  private final String realm;
  private final boolean isServerPrincipal;

  private KerberosName(String name, String host, String realm, boolean isServerPrincipal) {
    this.name = name;
    this.host = host;
    this.realm = realm;
    this.isServerPrincipal = isServerPrincipal;
  }

  public static KerberosName parsePrincipalName(String name) {
    String[] parts = name.split("[/@]");
    if (parts.length < 2 || parts.length > 3) {
      String errorMsg = String.format("Malformed kerberos pricipal name: %s,"
        + " it should be in the format of name/host@REALM or name@REALM", name);
      throw new UnknownFormatConversionException(errorMsg);
    }
    if (parts.length == 2) {
      return new KerberosName(parts[0], "", parts[1], false);
    }
    return new KerberosName(parts[0], parts[1], parts[2], true);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(name);
    if (isServerPrincipal) {
      builder.append("/").append(host);
    }
    builder.append("@").append(realm);
    return builder.toString();
  }

  public String getName() {
    return name;
  }

  public String getRealm() {
    return realm;
  }

  public String getHost() {
    return host;
  }
}
