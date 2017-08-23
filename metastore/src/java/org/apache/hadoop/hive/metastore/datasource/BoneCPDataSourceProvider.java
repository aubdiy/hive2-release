/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.datasource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataSourceProvider for the BoneCP connection pool.
 */
public class BoneCPDataSourceProvider implements DataSourceProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BoneCPDataSourceProvider.class);

  public static final String BONECP = "bonecp";
  private static final String CONNECTION_TIMEOUT_PROPERTY= "bonecp.connectionTimeoutInMs";
  private static final String PARTITION_COUNT_PROPERTY= "bonecp.partitionCount";

  @Override
  public DataSource create(Configuration hdpConfig) throws SQLException {

    LOG.debug("Creating BoneCP connection pool for the MetaStore");

    String driverUrl = HiveConf.getVar(hdpConfig, HiveConf.ConfVars.METASTORECONNECTURLKEY);
    String user = HiveConf.getVar(hdpConfig, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
    String passwd;
    try {
      passwd = ShimLoader.getHadoopShims().getPassword(hdpConfig,
        HiveConf.ConfVars.METASTOREPWD.varname);
    } catch (IOException err) {
      throw new SQLException("Error getting metastore password", err);
    }
    int maxPoolSize = hdpConfig.getInt(
        HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_MAX_CONNECTIONS.varname,
        // upstream MetastoreConf - which is missing from this HDP line - is a separate class from HiveConf with
        // a different behavior: HiveConf sets only the default int/long value based on the type of the default,
        // while MetastoreConf loads a long value and converts it to an int. The below solution works similarly:
        HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_MAX_CONNECTIONS.defaultIntVal == -1 ?
          ((Long)HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_MAX_CONNECTIONS.defaultLongVal).intValue()
          : HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_MAX_CONNECTIONS.defaultIntVal);

    Properties properties = DataSourceProvider.getPrefixedProperties(hdpConfig, BONECP);
    long connectionTimeout = hdpConfig.getLong(CONNECTION_TIMEOUT_PROPERTY, 30000L);
    String partitionCount = properties.getProperty(PARTITION_COUNT_PROPERTY, "1");

    BoneCPConfig config = null;
    try {
      config = new BoneCPConfig(properties);
    } catch (Exception e) {
      throw new SQLException("Cannot create BoneCP configuration: ", e);
    }
    config.setJdbcUrl(driverUrl);
    //if we are waiting for connection for a long time, something is really wrong
    //better raise an error than hang forever
    //see DefaultConnectionStrategy.getConnectionInternal()
    config.setConnectionTimeoutInMs(connectionTimeout);
    config.setMaxConnectionsPerPartition(maxPoolSize);
    config.setPartitionCount(Integer.parseInt(partitionCount));
    config.setUser(user);
    config.setPassword(passwd);
    return new BoneCPDataSource(config);
  }

  @Override
  public boolean mayReturnClosedConnection() {
    // See HIVE-11915 for details
    return true;
  }

  @Override
  public boolean supports(Configuration configuration) {
    String poolingType =
        configuration.get(
            HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE.varname).toLowerCase();
    if (BONECP.equals(poolingType)) {
      int boneCpPropsNr = DataSourceProvider.getPrefixedProperties(configuration, BONECP).size();
      LOG.debug("Found " + boneCpPropsNr + " nr. of bonecp specific configurations");
      return boneCpPropsNr > 0;
    }
    LOG.debug("Configuration requested " + poolingType + " pooling, BoneCpDSProvider exiting");
    return false;
  }
}
