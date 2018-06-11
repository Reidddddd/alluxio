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

package alluxio.security.group.provider;

import alluxio.Configuration;
import alluxio.OnlineReconfigManager;
import alluxio.OnlineReconfigObserver;
import alluxio.PropertyKey;
import alluxio.security.group.GroupMappingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User group mapping based on configuration setting.
 */
public class ConfigBasedUserGroupMapping implements GroupMappingService, OnlineReconfigObserver {
  private final static Logger LOG = LoggerFactory.getLogger(ConfigBasedUserGroupMapping.class);

  private final static String USER_SEPARATOR = ";";
  private final static String USER_GROUPS_SEPARATOR = "=";
  private final static String GROUPS_SEPARATOR = ",";

  private final Map<String, List<String>> mUserAndGroups;
  private String mOriginUserGroupMapping;

  public ConfigBasedUserGroupMapping() {
    mUserAndGroups = new HashMap<>();
    initialize();
    OnlineReconfigManager.getInstance().registerObserver(this);
  }

  private void initialize() {
    mOriginUserGroupMapping = Configuration.get(PropertyKey.SECURITY_USER_GROUP_MAPPING);
    for (String userGroups : mOriginUserGroupMapping.split(USER_SEPARATOR)) {
      String[] userAndGroups = userGroups.split(USER_GROUPS_SEPARATOR);
      String user = userAndGroups[0];
      String groups = userAndGroups[1];
      mUserAndGroups.put(user, Arrays.asList(groups.split(GROUPS_SEPARATOR)));
    }
    for (Map.Entry<String, List<String>> userGroups : mUserAndGroups.entrySet()) {
      LOG.info("User {} belongs to groups {}.",
          userGroups.getKey(), userGroups.getValue().toString());
    }
  }

  @Override
  public List<String> getGroups(String user) {
    if (!mUserAndGroups.containsKey(user)) {
      return Collections.emptyList();
    }
    return mUserAndGroups.get(user);
  }

  @Override
  public boolean needUpdate() {
    return !mOriginUserGroupMapping.equals(
        Configuration.get(PropertyKey.SECURITY_USER_GROUP_MAPPING));
  }

  @Override
  public void onConfigurationChange() {
    mUserAndGroups.clear();
    initialize();
  }

  @Override
  public String toString() {
    return "ConfigBasedUserGroupMapping";
  }
}
