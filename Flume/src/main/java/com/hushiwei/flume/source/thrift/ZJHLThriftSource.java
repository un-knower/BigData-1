package com.hushiwei.flume.source.thrift;

/**
 * Created by HuShiwei on 2016/8/21 0021.
 */

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLServerSocket;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.FlumeAuthenticator;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.flume.thrift.ThriftSourceProtocol.Processor;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.AbstractNonblockingServer;
import org.apache.thrift.server.AbstractNonblockingServer.AbstractNonblockingServerArgs;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.AbstractServerArgs;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.*;

public class ZJHLThriftSource extends AbstractSource
        implements Configurable, EventDrivenSource {
    public static final Logger logger = Logger.getLogger(ZJHLThriftSource.class);
    public static final String CONFIG_THREADS = "threads";
    public static final String CONFIG_BIND = "bind";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_PROTOCOL = "protocol";
    public static final String BINARY_PROTOCOL = "binary";
    public static final String COMPACT_PROTOCOL = "compact";
    private static final String SSL_KEY = "ssl";
    private static final String KEYSTORE_KEY = "keystore";
    private static final String KEYSTORE_PASSWORD_KEY = "keystore-password";
    private static final String KEYSTORE_TYPE_KEY = "keystore-type";
    private static final String EXCLUDE_PROTOCOLS = "exclude-protocols";
    private static final String KERBEROS_KEY = "kerberos";
    private static final String AGENT_PRINCIPAL = "agent-principal";
    private static final String AGENT_KEYTAB = "agent-keytab";
    private Integer port;
    private String bindAddress;
    private int maxThreads = 0;
    private SourceCounter sourceCounter;
    private TServer server;
    private ExecutorService servingExecutor;
    private String protocol;
    private String keystore;
    private String keystorePassword;
    private String keystoreType;
    private final List<String> excludeProtocols = new LinkedList();
    private boolean enableSsl = false;
    private boolean enableKerberos = false;
    private String principal;
    private FlumeAuthenticator flumeAuth;
    private AtomicBoolean stop = new AtomicBoolean(false);

    public void configure(Context context) {
        logger.info("Configuring thrift source.");
        this.port = context.getInteger("port");
        Preconditions.checkNotNull(this.port, "Port must be specified for Thrift Source.");

        this.bindAddress = context.getString("bind");
        Preconditions.checkNotNull(this.bindAddress, "Bind address must be specified for Thrift Source.");
        try {
            this.maxThreads = context.getInteger("threads", Integer.valueOf(0)).intValue();
            this.maxThreads = (this.maxThreads <= 0 ? 2147483647 : this.maxThreads);
        } catch (NumberFormatException e) {
            logger.warn("Thrift source's \"threads\" property must specify an integer value: " +
                    context.getString("threads"));
        }

        if (this.sourceCounter == null) {
            this.sourceCounter = new SourceCounter(getName());
        }

        this.protocol = context.getString("protocol");
        if (this.protocol == null) {
            this.protocol = "compact";
        }
        Preconditions.checkArgument(
                (this.protocol.equalsIgnoreCase("binary")) ||
                        (this.protocol.equalsIgnoreCase("compact")),
                "binary or compact are the only valid Thrift protocol types to choose from.");

        this.enableSsl = context.getBoolean("ssl", Boolean.valueOf(false)).booleanValue();
        if (this.enableSsl) {
            this.keystore = context.getString("keystore");
            this.keystorePassword = context.getString("keystore-password");
            this.keystoreType = context.getString("keystore-type", "JKS");
            String excludeProtocolsStr = context.getString("exclude-protocols");
            if (excludeProtocolsStr == null) {
                this.excludeProtocols.add("SSLv3");
            } else {
                this.excludeProtocols.addAll(Arrays.asList(excludeProtocolsStr.split(" ")));
                if (!this.excludeProtocols.contains("SSLv3")) {
                    this.excludeProtocols.add("SSLv3");
                }
            }
            Preconditions.checkNotNull(this.keystore,
                    "keystore must be specified when SSL is enabled");
            Preconditions.checkNotNull(this.keystorePassword,
                    "keystore-password must be specified when SSL is enabled");
            try {
                KeyStore ks = KeyStore.getInstance(this.keystoreType);
                ks.load(new FileInputStream(this.keystore), this.keystorePassword.toCharArray());
            } catch (Exception ex) {
                throw new FlumeException(
                        "Thrift source configured with invalid keystore: " + this.keystore, ex);
            }
        }

        this.principal = context.getString("agent-principal");
        String keytab = context.getString("agent-keytab");
        this.enableKerberos = context.getBoolean("kerberos", Boolean.valueOf(false)).booleanValue();
        this.flumeAuth = FlumeAuthenticationUtil.getAuthenticator(this.principal, keytab);
        if (this.enableKerberos) {
            if (!this.flumeAuth.isAuthenticated()) {
                throw new FlumeException("Authentication failed in Kerberos mode for principal " +
                        this.principal + " keytab " + keytab);
            }
            this.flumeAuth.startCredentialRefresher();
        }
    }

    public void start() {
        logger.info("Starting thrift source");

        this.server = getTThreadedSelectorServer();

        if (this.server == null) {
            this.server = getTThreadPoolServer();
        }

        this.servingExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("Flume Thrift Source I/O Boss")
                .build());

        this.servingExecutor.submit(new Runnable() {
            public void run() {
                ZJHLThriftSource.this.flumeAuth.execute(
                        new PrivilegedAction() {
                            public Object run() {
                                ZJHLThriftSource.this.server.serve();
                                return null;
                            }
                        });
            }
        });
        long timeAfterStart = System.currentTimeMillis();
        while (!this.server.isServing()) {
            try {
                if (System.currentTimeMillis() - timeAfterStart >= 10000L) {
                    throw new FlumeException("Thrift server failed to start!");
                }
                TimeUnit.MILLISECONDS.sleep(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlumeException("Interrupted while waiting for Thrift server to start.",
                        e);
            }
        }
        this.sourceCounter.start();
        logger.info("Started Thrift source.");
        super.start();
    }

    private String getkeyManagerAlgorithm() {
        String algorithm = Security.getProperty(
                "ssl.KeyManagerFactory.algorithm");
        return algorithm != null ?
                algorithm : KeyManagerFactory.getDefaultAlgorithm();
    }

    private TServerTransport getSSLServerTransport() {
        try {
            TSSLTransportFactory.TSSLTransportParameters params =
                    new TSSLTransportFactory.TSSLTransportParameters();

            params.setKeyStore(this.keystore, this.keystorePassword, getkeyManagerAlgorithm(), this.keystoreType);
            TServerTransport transport = TSSLTransportFactory.getServerSocket(
                    this.port.intValue(), 120000, InetAddress.getByName(this.bindAddress), params);

            ServerSocket serverSock = ((TServerSocket) transport).getServerSocket();
            if ((serverSock instanceof SSLServerSocket)) {
                SSLServerSocket sslServerSock = (SSLServerSocket) serverSock;
                List enabledProtocols = new ArrayList();
                for (String protocol : sslServerSock.getEnabledProtocols()) {
                    if (!this.excludeProtocols.contains(protocol)) {
                        enabledProtocols.add(protocol);
                    }
                }
                sslServerSock.setEnabledProtocols((String[]) enabledProtocols.toArray(new String[0]));
            }
            return transport;
        } catch (Throwable throwable) {
        }
        throw new FlumeException("Cannot start Thrift source.");
    }

    private TServerTransport getTServerTransport() {
        try {
            return new TServerSocket(
                    new InetSocketAddress(this.bindAddress, this.port.intValue()));
        } catch (Throwable throwable) {
        }
        throw new FlumeException("Cannot start Thrift source.");
    }

    private TProtocolFactory getProtocolFactory() {
        if (this.protocol.equals("binary")) {
            logger.info("Using TBinaryProtocol");
            return new TBinaryProtocol.Factory();
        }
        logger.info("Using TCompactProtocol");
        return new TCompactProtocol.Factory();
    }

    private TServer getTThreadedSelectorServer() {
        if ((this.enableSsl) || (this.enableKerberos)) {
            return null;
        }

        try {
            Class serverClass = Class.forName("org.apache.thrift.server.TThreadedSelectorServer");

            Class argsClass = Class.forName("org.apache.thrift.server.TThreadedSelectorServer$Args");

            TServerTransport serverTransport = new TNonblockingServerSocket(
                    new InetSocketAddress(this.bindAddress, this.port.intValue()));

            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                    "Flume Thrift IPC Thread %d").build();
            ExecutorService sourceService;
            if (this.maxThreads == 0)
                sourceService = Executors.newCachedThreadPool(threadFactory);
            else {
                sourceService = Executors.newFixedThreadPool(this.maxThreads, threadFactory);
            }
            TServer.AbstractServerArgs args =
                    (AbstractNonblockingServer.AbstractNonblockingServerArgs) argsClass
                            .getConstructor(new Class[]{
                                    TNonblockingServerTransport.class})
                            .newInstance(new Object[]{
                                    serverTransport});
            Method m = argsClass.getDeclaredMethod("executorService", new Class[]{
                    ExecutorService.class});
            m.invoke(args, new Object[]{sourceService});

            populateServerParams(args);

            this.server = ((TServer) serverClass.getConstructor(new Class[]{argsClass}).newInstance(new Object[]{args}));
        } catch (ClassNotFoundException e) {
            return null;
        } catch (Throwable ex) {
            throw new FlumeException("Cannot start Thrift Source.", ex);
        }
        TServer.AbstractServerArgs args;
        Class argsClass;
        Class serverClass;
        return this.server;
    }

    private TServer getTThreadPoolServer() {
        TServerTransport serverTransport;
        if (this.enableSsl)
            serverTransport = getSSLServerTransport();
        else {
            serverTransport = getTServerTransport();
        }
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.maxWorkerThreads(this.maxThreads);
        populateServerParams(serverArgs);
        return new TThreadPoolServer(serverArgs);
    }

    private void populateServerParams(TServer.AbstractServerArgs args) {
        args.protocolFactory(getProtocolFactory());

        if (this.enableKerberos)
            args.transportFactory(getSASLTransportFactory());
        else {
            args.transportFactory(new TFastFramedTransport.Factory());
        }

        args.processor(
                new ThriftSourceProtocol.Processor(new ThriftSourceHandler(getChannelProcessor(), this.sourceCounter, this.stop)));
    }

    private TTransportFactory getSASLTransportFactory() {
        String[] names;
        try {
            names = FlumeAuthenticationUtil.splitKerberosName(this.principal);
        } catch (IOException e) {
            throw new FlumeException(
                    "Error while trying to resolve Principal name - " + this.principal, e);
        }
        Map saslProperties = new HashMap();
        saslProperties.put("javax.security.sasl.qop", "auth");
        TSaslServerTransport.Factory saslTransportFactory =
                new TSaslServerTransport.Factory();
        saslTransportFactory.addServerDefinition(
                "GSSAPI", names[0], names[1], saslProperties,
                FlumeAuthenticationUtil.getSaslGssCallbackHandler());
        return saslTransportFactory;
    }

    public void stop() {
        if ((this.server != null) && (this.server.isServing())) {
            this.server.stop();
        }

        if (this.servingExecutor != null) {
            this.servingExecutor.shutdown();
            try {
                if (!this.servingExecutor.awaitTermination(5L, TimeUnit.SECONDS))
                    this.servingExecutor.shutdownNow();
            } catch (InterruptedException e) {
                throw new FlumeException("Interrupted while waiting for server to be shutdown.");
            }
        }

        this.stop.set(true);
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.sourceCounter.stop();
        super.stop();
    }
}
