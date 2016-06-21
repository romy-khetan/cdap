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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.internal.app.store.remote.MethodArgument;
import co.cask.http.AbstractHttpHandler;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implements common functionality for reading method arguments and deserializing them.
 */
public class AbstractRemoteStoreHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type METHOD_ARGUMENT_LIST_TYPE = new TypeToken<List<MethodArgument>>() { }.getType();

  // we don't share the same version as other handlers in app fabric, so we can upgrade/iterate faster
  protected static final String VERSION = "/v1";

  protected List<MethodArgument> parseArguments(HttpRequest request) {
    String body = request.getContent().toString(Charsets.UTF_8);
    return GSON.fromJson(body, METHOD_ARGUMENT_LIST_TYPE);
  }

  @Nullable
  protected <T> T deserialize(@Nullable MethodArgument argument) throws ClassNotFoundException {
    if (argument == null) {
      return null;
    }
    JsonElement value = argument.getValue();
    if (value == null) {
      return null;
    }
    return GSON.<T>fromJson(value, Class.forName(argument.getType()));
  }
}
