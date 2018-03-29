/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.store.profile.ProfileStore;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.profile.ProfileInfo;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} for managing profiles.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ProfileHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();

  private final ProfileStore profileStore;

  @Inject
  public ProfileHttpHandler(ProfileStore profileStore) {
    this.profileStore = profileStore;
  }

  /**
   * List the profiles in the given namespace. By default the results will not contain profiles in system scope.
   */
  @GET
  @Path("/profiles")
  public void getProfiles(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @QueryParam("includeSystem") @DefaultValue("false") String includeSystem) throws IOException {
    List<Profile> profiles = new ArrayList<>();
    NamespaceId namespace = new NamespaceId(namespaceId);
    boolean include = Boolean.valueOf(includeSystem);
    if (include && !namespace.equals(NamespaceId.SYSTEM)) {
      profiles.addAll(profileStore.getProfiles(NamespaceId.SYSTEM));
    }
    profiles.addAll(profileStore.getProfiles(namespace));
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profiles));
  }

  /**
   * Get the information about a specific profile.
   */
  @GET
  @Path("/profiles/{profile-name}")
  public void getProfile(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("profile-name") String profileName) throws NotFoundException, IOException {
    ProfileId profileId = new ProfileId(namespaceId, profileName);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(profileStore.getProfile(profileId)));
  }

  /**
   * Write a profile in a namespace.
   */
  @PUT
  @Path("/profiles/{profile-name}")
  public void writeProfile(
    FullHttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
    @PathParam("profile-name") String profileName) throws BadRequestException, IOException, AlreadyExistsException {
    ProfileInfo profileInfo;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      profileInfo = GSON.fromJson(reader, ProfileInfo.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
    ProfileId profileId = new ProfileId(namespaceId, profileName);
    Profile profile =
      new Profile(profileName, profileInfo.getDescription(),
                  profileId.getNamespaceId().equals(NamespaceId.SYSTEM) ? EntityScope.SYSTEM : EntityScope.USER,
                  profileInfo.getProvisionerInfo());
    profileStore.add(profileId, profile);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Delete a profile from a namespace. A profile must be in the disabled state before it can be deleted.
   * Before a profile can be deleted, it cannot be assigned to any program or schedule,
   * and it cannot be in use by any running program.
   */
  @DELETE
  @Path("/profiles/{profile-name}")
  public void deleteProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName) throws NotFoundException, ConflictException {
    // TODO: implement the method, though we have ProfileStore, we have to make sure the profile is not associated to
    // any program and schedules before deleting
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Disable the profile, so that no new program runs can use it,
   * and no new schedules/programs can be assigned to it.
   */
  @POST
  @Path("/profiles/{profile-name}/disable")
  public void disableProfile(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("profile-name") String profileName) throws NotFoundException {
    // TODO: implement the method
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Enable the profile, so that programs/schedules can be assigned to it.
   */
  @POST
  @Path("/profiles/{profile-name}/enable")
  public void enableProfile(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("profile-name") String profileName) throws NotFoundException {
    // TODO: implement the method
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
