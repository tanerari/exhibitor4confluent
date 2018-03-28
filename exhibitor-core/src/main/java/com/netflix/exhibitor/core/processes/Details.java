/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.exhibitor.core.processes;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.EncodedConfigParser;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

class Details
{
    final File confluentDirectory;
    final File configDirectory;
    final File dataDirectory;
    final File logDirectory;
    final Properties properties;
    final String confluentStartupLevel;
    final File ksqlDirectory;
    final boolean ksqlEnabled;

    Details(Exhibitor exhibitor) throws IOException
    {
        InstanceConfig config = exhibitor.getConfigManager().getConfig();

        this.confluentDirectory = new File(config.getString(StringConfigs.CONFLUENT_INSTALL_DIRECTORY));
        this.configDirectory = new File(this.confluentDirectory, "current/zookeeper");
        this.dataDirectory = new File(config.getString(StringConfigs.ZOOKEEPER_DATA_DIRECTORY));

        String cfgLogDir = config.getString(StringConfigs.ZOOKEEPER_LOG_DIRECTORY);
        this.logDirectory = (cfgLogDir.trim().length() > 0) ? new File(cfgLogDir) : this.dataDirectory;

        this.confluentStartupLevel = config.getString(StringConfigs.CONFLUENT_STARTUP_LEVEL);
        this.ksqlDirectory = new File(config.getString(StringConfigs.KSQL_INSTALL_DIRECTORY));
        this.ksqlEnabled = config.getInt(IntConfigs.KSQL_ENABLED) > 0;

        properties = new Properties();
        if ( isValid() ) {
            EncodedConfigParser     parser = new EncodedConfigParser(exhibitor.getConfigManager().getConfig().getString(StringConfigs.ZOO_CFG_EXTRA));
            for ( EncodedConfigParser.FieldValue fv : parser.getFieldValues() ) {
                properties.setProperty(fv.getField(), fv.getValue());
            }
            properties.setProperty("dataDir", dataDirectory.getPath());
            properties.setProperty("dataLogDir", this.logDirectory.getPath());
        }
    }

    boolean isValid()
    {
        return isValidPath(confluentDirectory)
            && isValidPath(configDirectory)
            && isValidPath(dataDirectory)
            && isValidPath(logDirectory)
            ;
    }

    public boolean isValidPath(File directory)
    {
        return directory.getPath().length() > 0;
    }
}
