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

import com.google.common.io.Files;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.state.ServerSpec;
import com.netflix.exhibitor.core.state.ServerType;
import com.netflix.exhibitor.core.state.UsState;
import org.apache.curator.utils.CloseableUtils;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.*;

public class StandardProcessOperations implements ProcessOperations
{
    private final Exhibitor exhibitor;

    private static final int    SLEEP_KILL_TIME_MS = 100;
    private static final int    SLEEP_KILL_WAIT_COUNT = 3;

    private static final List<String> ksqlMinLevel = Arrays.asList("kafka-rest", "connect");
    private boolean isKsqlRunning;

    public StandardProcessOperations(Exhibitor exhibitor) throws IOException
    {
        this.exhibitor = exhibitor;
    }

    @Override
    public void cleanupInstance() throws Exception
    {
        exhibitor.getLog().add(ActivityLog.Type.INFO, "Zookeeper CleanUp operation is managed automatically with parameter 'autopurge.purgeInterval'");
    }

    @Override
    public void killInstance() throws Exception
    {
        exhibitor.getLog().add(ActivityLog.Type.INFO, "Attempting to start/restart 'Confluent Platform'");

        exhibitor.getProcessMonitor().destroy(ProcessTypes.CONFLUENT);

        String zkPid = getPid("QuorumPeerMain");
        if ( zkPid == null ) {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "jps didn't find instance - assuming ZK is not running");
        }else {
            waitForKill(zkPid, "QuorumPeerMain");
        }

        if( this.isKsqlRunning ) {
            String ksqlPid = getPid("KsqlRestApplication");
            if (ksqlPid == null) {
                exhibitor.getLog().add(ActivityLog.Type.INFO, "jps didn't find instance - assuming KSQL-Server is not running");
            } else {
                waitForKill(ksqlPid, "KsqlRestApplication");
            }
            this.isKsqlRunning = false;
        }
    }

    private ProcessBuilder buildConfluentScript(String operation, String target) throws IOException
    {
        Details details = new Details(exhibitor);
        File binDirectory = new File(details.confluentDirectory, "bin");
        File confluentScript = new File(binDirectory, "confluent");
        return new ProcessBuilder(confluentScript.getAbsolutePath(), operation, target).directory(binDirectory.getParentFile());
    }

    private ProcessBuilder buildKsqlStartScript() throws IOException
    {
        Details details = new Details(exhibitor);
        File binDirectory = new File(details.ksqlDirectory, "bin");
        File configFile = new File(details.ksqlDirectory, "config/ksqlserver.properties");
        File ksqlScript = new File(binDirectory, "ksql-server-start");
        return new ProcessBuilder(ksqlScript.getAbsolutePath(), "-daemon", configFile.getAbsolutePath()).directory(binDirectory.getParentFile());
    }

    private ProcessBuilder buildKsqlStopScript() throws IOException
    {
        Details details = new Details(exhibitor);
        File binDirectory = new File(details.ksqlDirectory, "bin");
        File ksqlScript = new File(binDirectory, "ksql-server-stop");
        return new ProcessBuilder(ksqlScript.getAbsolutePath()).directory(binDirectory.getParentFile());
    }

    @Override
    public void startInstance() throws Exception
    {
        Details details = new Details(exhibitor);
        prepareConfigFile(details);
        ProcessBuilder confluentBuilder = buildConfluentScript("start", details.confluentStartupLevel);
        Process confluentProcess = confluentBuilder.start();
        exhibitor.getProcessMonitor().monitor(ProcessTypes.CONFLUENT, confluentProcess, null, ProcessMonitor.Mode.LEAVE_RUNNING_ON_INTERRUPT, ProcessMonitor.Streams.BOTH);
        exhibitor.getLog().add(ActivityLog.Type.INFO, "A new process started via: " + confluentBuilder.command().get(0) + " " + confluentBuilder.command().get(1) + " " + confluentBuilder.command().get(2));
        int result = confluentProcess.waitFor();
        if( result == 0 && details.ksqlEnabled && !this.isKsqlRunning && details.isValidPath(details.ksqlDirectory) && ksqlMinLevel.contains(details.confluentStartupLevel)) {
            ProcessBuilder ksqlBuilder = buildKsqlStartScript();
            exhibitor.getProcessMonitor().monitor(ProcessTypes.KSQL, ksqlBuilder.start(), null, ProcessMonitor.Mode.LEAVE_RUNNING_ON_INTERRUPT, ProcessMonitor.Streams.BOTH);
            exhibitor.getLog().add(ActivityLog.Type.INFO, "A new process started via: " + ksqlBuilder.command().get(0) + " " + ksqlBuilder.command().get(1) + " " + ksqlBuilder.command().get(2));
            this.isKsqlRunning = true;
        }
    }

    private void prepareConfigFile(Details details) throws IOException
    {
        UsState usState = new UsState(exhibitor);

        if (! details.dataDirectory.exists()){
            details.dataDirectory.mkdir();
        }
        File idFile = new File(details.dataDirectory, "myid");
        if ( usState.getUs() != null ) {
            Files.createParentDirs(idFile);
            String id = String.format("%d\n", usState.getUs().getServerId());
            Files.write(id.getBytes(), idFile);
        } else {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Starting in standalone mode");
            if ( idFile.exists() && !idFile.delete() ) {
                exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not delete ID file: " + idFile);
            }
        }
        Properties localProperties = new Properties();
        localProperties.putAll(details.properties);
        localProperties.setProperty("clientPort", Integer.toString(usState.getConfig().getInt(IntConfigs.CLIENT_PORT)));
        String portSpec = String.format(":%d:%d", usState.getConfig().getInt(IntConfigs.CONNECT_PORT), usState.getConfig().getInt(IntConfigs.ELECTION_PORT));
        for ( ServerSpec spec : usState.getServerList().getSpecs() ) {
            localProperties.setProperty("server." + spec.getServerId(), spec.getHostname() + portSpec + spec.getServerType().getZookeeperConfigValue());
        }
        if ( (usState.getUs() != null) && (usState.getUs().getServerType() == ServerType.OBSERVER) ) {
            localProperties.setProperty("peerType", "observer");
        }
        if (! details.configDirectory.exists()){
            details.configDirectory.mkdir();
        }
        File configFile = new File(details.configDirectory, "zookeeper.properties");
        OutputStream out = new BufferedOutputStream(new FileOutputStream(configFile));
        try {
            localProperties.store(out, "Auto-generated by Exhibitor - " + new Date());
        }finally {
            CloseableUtils.closeQuietly(out);
        }
    }

    private String getPid(String jpsName) throws IOException
    {
        ProcessBuilder builder = new ProcessBuilder("jps");
        Process jpsProcess = builder.start();
        String pid = null;
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(jpsProcess.getInputStream()));
            for(;;) {
                String  line = in.readLine();
                if ( line == null ) {
                    break;
                }
                String[]  components = line.split("[ \t]");
                if ( (components.length == 2) && components[1].equals(jpsName) ) {
                    pid = components[0];
                    break;
                }
            }
        }finally {
            CloseableUtils.closeQuietly(jpsProcess.getErrorStream());
            CloseableUtils.closeQuietly(jpsProcess.getInputStream());
            CloseableUtils.closeQuietly(jpsProcess.getOutputStream());
            jpsProcess.destroy();
        }
        return pid;
    }

    private void waitForKill(String pid, String jpsName) throws IOException, InterruptedException
    {
        boolean success = false;
        for ( int i = 0; i < SLEEP_KILL_WAIT_COUNT; ++i ) {
            internalKill(pid, jpsName, i > 0);
            Thread.sleep(i * SLEEP_KILL_TIME_MS);
            if ( !pid.equals(getPid(jpsName)) ) {
                success = true;
                break;  // assume it was successfully killed
            }
        }
        if ( !success ) {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Could not kill '" + jpsName + "' process: " + pid);
        }
    }

    private void internalKill(String pid, String jpsName, boolean force) throws IOException, InterruptedException
    {
        ProcessBuilder builder = null;
        if( "QuorumPeerMain".equals(jpsName) ) {
            builder = force ? new ProcessBuilder("kill", "-9", pid) : buildConfluentScript("stop", "zookeeper");
        } else if ("KsqlRestApplication".equals(jpsName)) {
            builder = force ? new ProcessBuilder("kill", "-9", pid) : buildKsqlStopScript();
        }
        if( builder != null ) {
            try {
                int result = builder.start().waitFor();
                exhibitor.getLog().add(ActivityLog.Type.INFO, "Kill attempted result: " + result);
            } catch (InterruptedException exc) {
                // don't reset thread interrupted status
                exhibitor.getLog().add(ActivityLog.Type.ERROR, "Process interrupted while running: kill -9 " + pid);
                throw exc;
            }
        }
    }
}
