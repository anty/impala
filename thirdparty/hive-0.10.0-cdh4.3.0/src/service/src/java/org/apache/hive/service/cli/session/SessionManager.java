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

package org.apache.hive.service.cli.session;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.log.LogManager;
import org.apache.hive.service.cli.operation.OperationManager;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  private HiveConf hiveConf;
  private final Map<SessionHandle, HiveSession> handleToSession = new HashMap<SessionHandle, HiveSession>();
  private OperationManager operationManager = new OperationManager();
  private LogManager logManager = new LogManager();
  private static final Object sessionMapLock = new Object();

  public SessionManager() {
    super("SessionManager");
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    operationManager = new OperationManager();
    operationManager.setSessionManager(this);
    addService(operationManager);

    logManager = new LogManager();
    logManager.setSessionManager(this);

    addService(logManager);

    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // TODO
  }

  @Override
  public synchronized void stop() {
    // TODO
    super.stop();
  }

  public SessionHandle openSession(String username, String password, Map<String, String> sessionConf,
          boolean withImpersonation, String delegationToken) throws HiveSQLException {
    HiveSession session;
    if (username == null) {
      username = threadLocalUserName.get();
    }

    if (withImpersonation) {
          HiveSessionImplwithUGI hiveSessionUgi = new HiveSessionImplwithUGI(username, password, sessionConf,
              threadLocalIpAddress.get(), delegationToken);
          session = (HiveSession)HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
          hiveSessionUgi.setProxySession(session);
    } else {
      session = new HiveSessionImpl(username, password, sessionConf, threadLocalIpAddress.get());
    }

    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    session.setLogManager(logManager);
    synchronized(sessionMapLock) {
      handleToSession.put(session.getSessionHandle(), session);
    }
    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      throw new HiveSQLException("Failed to execute session hooks", e);
    }
    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session;
    synchronized(sessionMapLock) {
      session = handleToSession.remove(sessionHandle);
    }
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    session.close();
  }


  public HiveSession getSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session;
    synchronized(sessionMapLock) {
      session = handleToSession.get(sessionHandle);
    }
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  public LogManager getLogManager() {
    return logManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  private void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  private void clearUserName() {
    threadLocalUserName.remove();
  }

  public void clearThreadLocals() {
    clearIpAddress();
    clearUserName();
  }

  // execute session hooks
  private void executeSessionHooks(HiveSession session) throws Exception {
    String hookList = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK).trim();
    if (hookList == null || hookList.isEmpty()) {
      return;
    }

    String[] hookClasses = hookList.split(",");
    for (String hookClass : hookClasses) {
      HiveSessionHook sessionHook = (HiveSessionHook)
          Class.forName(hookClass.trim(), true, JavaUtils.getClassLoader()).newInstance();
      sessionHook.run(new HiveSessionHookContextImpl(session));
    }
  }

}
