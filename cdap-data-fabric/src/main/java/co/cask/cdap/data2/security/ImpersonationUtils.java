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

package co.cask.cdap.data2.security;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 *
 */
public final class ImpersonationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationUtils.class);

  private ImpersonationUtils() { }

  /**
   * Returns the path to a keytab file. If it's not a local file, copies it to local file system first.
   *
   * @param locationFactory the location factory to use to retrieve the keytab file, if the file is not already local
   * @param keytabURI the URI of the keytab file
   * @param tempDir a directory in which to localize the keytab file, if necessary
   * @return the path to a local keytab file
   */
  public static String localizeKeytab(LocationFactory locationFactory, URI keytabURI, File tempDir) throws IOException {
    // if scheme is not specified, assume its local file
    if (keytabURI.getScheme() == null || "file".equals(keytabURI.getScheme())) {
      return keytabURI.getPath();
    }

    // create a local file with restricted permissions
    // only allow the owner to read/write, since it contains credentials
    FileAttribute<Set<PosixFilePermission>> ownerOnlyAttrs =
      PosixFilePermissions.asFileAttribute(ImmutableSet.of(PosixFilePermission.OWNER_WRITE,
                                                           PosixFilePermission.OWNER_READ));
    Path localKeytabFile = java.nio.file.Files.createFile(Paths.get(tempDir.getAbsolutePath(), "keytab.localized"),
                                                          ownerOnlyAttrs);

    // copy to this local file
    final Location location = locationFactory.create(keytabURI);
    LOG.info("Copying keytab file from {} to {}", location, localKeytabFile);
    Files.copy(new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return location.getInputStream();
      }
    }, localKeytabFile.toFile());


    return localKeytabFile.toFile().getPath();
  }

  /**
   * Helper function, to unwrap any exceptions that were wrapped
   * by {@link UserGroupInformation#doAs(PrivilegedExceptionAction)}
   */
  public static <T> T doAs(UserGroupInformation ugi, final Callable<T> callable) throws Exception {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
    } catch (UndeclaredThrowableException e) {
      // UserGroupInformation#doAs will wrap any checked exceptions, so unwrap and rethrow here
      Throwable wrappedException = e.getUndeclaredThrowable();
      Throwables.propagateIfPossible(wrappedException);

      if (wrappedException instanceof Exception) {
        throw (Exception) wrappedException;
      }
      // since PrivilegedExceptionAction#run can only throw Exception (besides runtime exception),
      // this should never happen
      LOG.warn("Unexpected exception while executing callable as {}.",
               ugi.getUserName(), wrappedException);
      throw Throwables.propagate(wrappedException);
    }
  }
}
