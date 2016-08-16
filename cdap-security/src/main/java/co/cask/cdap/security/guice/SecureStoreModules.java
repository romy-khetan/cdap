/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.guice;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.security.store.DefaultSecureStoreService;
import co.cask.cdap.security.store.DummySecureStore;
import co.cask.cdap.security.store.FileSecureStore;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

/**
 * Guice bindings for security store related classes.
 *
 * file - InMemory    - with password       - Valid, returns FileSecureStore
 *                    - without password    - Invalid, throws IllegalArgumentException. (Set password)
 *      - Standalone  - with password       - Valid, returns FileSecureStore
 *                    - without password    - Invalid, throws IllegalArgumentException. (Set password)
 *      - Distributed - with password       - Invalid, throws IllegalArgumentException. (mode not supported)
 *                    - without password    - Invalid, throws IllegalArgumentException. (mode not supported)
 *
 * kms  - InMemory    - > Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *                    - < Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *      - Standalone  - > Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *                    - < Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *      - Distributed - > Hadoop 2.6        - Valid, returns KMSSecureStore
 *                    - < Hadoop 2.6        - Invalid, throws IllegalArgumentException. (Hadoop unsupported)
 *
 * none  - Loads a dummy store. Logs an error message explaining how to setup secure store if any secure store
 *         operations are performed.
 *
 */
public class SecureStoreModules extends RuntimeModule {
  public static final String DELEGATE_SECURE_STORE = "delegateSecureStore";
  public static final String DELEGATE_SECURE_STORE_MANAGER = "delegateSecureStoreManager";
  private static final String KMS_BACKED = "kms";
  private static final String FILE_BACKED = "file";

  @Override
  public final Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SecureStore.class).to(DefaultSecureStoreService.class);
        bind(SecureStoreManager.class).to(DefaultSecureStoreService.class);
        bind(SecureStore.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE))
          .toProvider(new TypeLiteral<StoreProvider<SecureStore>>() { });
        bind(SecureStoreManager.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE_MANAGER))
          .toProvider(new TypeLiteral<StoreProvider<SecureStoreManager>>() { });
      }
    };
  }

  @Override
  public final Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SecureStore.class).to(DefaultSecureStoreService.class);
        bind(SecureStoreManager.class).to(DefaultSecureStoreService.class);
        bind(SecureStore.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE))
          .toProvider(new TypeLiteral<StoreProvider<SecureStore>>() { });
        bind(SecureStoreManager.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE_MANAGER))
          .toProvider(new TypeLiteral<StoreProvider<SecureStoreManager>>() { });
      }
    };
  }

  @Override
  public final Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SecureStore.class).to(DefaultSecureStoreService.class);
        bind(SecureStoreManager.class).to(DefaultSecureStoreService.class);
        bind(SecureStore.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE))
          .toProvider(new TypeLiteral<DistributedStoreProvider<SecureStore>>() { });
        bind(SecureStoreManager.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE_MANAGER))
          .toProvider(new TypeLiteral<DistributedStoreProvider<SecureStoreManager>>() { });
      }
    };
  }

  @Singleton
  private static final class DistributedStoreProvider<T> implements Provider<T> {
    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private DistributedStoreProvider(final CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
      boolean kmsBacked = KMS_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
      if (kmsBacked && isKMSCapable()) {
        try {
          return (T) injector.getInstance(Class.forName("co.cask.cdap.security.store.KMSSecureStore"));
        } catch (ClassNotFoundException e) {
          // KMSSecureStore could not be loaded
          throw new RuntimeException("CDAP KMS classes could not be loaded. " +
                                       "Please verify that CDAP is correctly installed");
        }
      }
      if (kmsBacked) {
        throw new IllegalArgumentException("Could not find classes required for supporting KMS based secure store. " +
                                             "KMS backed secure store depends on " +
                                             "org.apache.hadoop.crypto.key.kms.KMSClientProvider being available. " +
                                             "This is supported in Apache Hadoop 2.6.0 and up and on " +
                                             "distribution versions that are based on Apache Hadoop 2.6.0 and up.");
      }

      if (FILE_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER))) {
        throw new IllegalArgumentException("Only KMS based provider is supported in distributed mode. " +
                   "To be able to use secure store in a distributed environment you" +
                   "will need to use the Hadoop KMS based provider.");
      }
      return (T) injector.getInstance(DummySecureStore.class);
    }

    private static boolean isKMSCapable() {
      try {
        // Check if required KMS classes are present.
        Class.forName("org.apache.hadoop.crypto.key.kms.KMSClientProvider");
        return true;
      } catch (ClassNotFoundException ex) {
        // KMS is not supported.
        return false;
      }
    }
  }

  @Singleton
  private static final class StoreProvider<T> implements Provider<T> {
    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final Injector injector;

    @Inject
    private StoreProvider(final CConfiguration cConf, SConfiguration sConf, Injector injector) {
      this.cConf = cConf;
      this.sConf = sConf;
      this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
      boolean kmsBacked = KMS_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
      boolean fileBacked = FILE_BACKED.equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER));
      boolean validPassword = !Strings.isNullOrEmpty(sConf.get(Constants.Security.Store.FILE_PASSWORD));

      if (fileBacked && validPassword) {
        return (T) injector.getInstance(FileSecureStore.class);
      }
      if (fileBacked) {
        throw new IllegalArgumentException("File secure store password is not set. " +
                                             "Please set the \"security.store.file.password\" property in your " +
                                             "cdap-security.xml.");
      }
      if (kmsBacked) {
        throw new IllegalArgumentException("Only file based secure store is supported in InMemory/Standalone modes. " +
                                             "Please set the \"security.store.provider\" property in cdap-site.xml " +
                                             "to file and set the \"security.store.file.password\" property in " +
                                             "your cdap-security.xml.");
      }
      return (T) injector.getInstance(DummySecureStore.class);
    }
  }
}
