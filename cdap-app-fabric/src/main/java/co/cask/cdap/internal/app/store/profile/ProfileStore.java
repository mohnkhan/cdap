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

package co.cask.cdap.internal.app.store.profile;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

/**
 * Store for profile.
 */
public class ProfileStore {
  private static final DatasetId DATASET_ID = NamespaceId.SYSTEM.dataset("profile.meta");
  private static final String PROFILE_PREFIX = "p";
  private static final byte[] COL = Bytes.toBytes("c");
  private static final DatasetProperties TABLE_PROPERTIES =
    TableProperties.builder().setConflictDetection(ConflictDetection.COLUMN).build();

  private static final Gson GSON = new Gson();

  private final DatasetFramework datasetFramework;
  private final Transactional transactional;

  @Inject
  public ProfileStore(DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   txClient, DATASET_ID.getParent(),
                                                                   Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by profile store.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), DATASET_ID, TABLE_PROPERTIES);
  }

  /**
   * Get the profile information about the given profile
   *
   * @param profileId the id of the profile to look up
   * @return the profile information about the given profile
   * @throws IOException if there was an IO error looking up the profile
   * @throws NotFoundException if the profile is not found
   */
  public Profile getProfile(final ProfileId profileId) throws IOException, NotFoundException {
    return Transactionals.execute(transactional, context -> {
      byte[] value = getTable(context).get(getRowKey(profileId), COL);
      if (value == null) {
        throw new NotFoundException(profileId);
      }
      return GSON.fromJson(Bytes.toString(value), Profile.class);
    }, IOException.class, NotFoundException.class);
  }

  /**
   * Get the profile information in the given namespace
   *
   * @param namespaceId the id of the profile to look up
   * @return the list of profiles which is in this namespace
   * @throws IOException if there was an IO error looking up the profile
   */
  public List<Profile> getProfiles(final NamespaceId namespaceId) throws IOException {
    return Transactionals.execute(transactional, context -> {
      List<Profile> profiles = Lists.newArrayList();
      try (Scanner scanner = getTable(context).scan(scanProfiles(namespaceId))) {
        Row row;
        while ((row = scanner.next()) != null) {
          profiles.add(GSON.fromJson(Bytes.toString(row.get(COL)), Profile.class));
        }
      }
      return Collections.unmodifiableList(profiles);
    }, IOException.class);
  }

  /**
   * Add the profile to the profile store.
   *
   * @param profileId the id of the profile to add
   * @param profile the information of the profile
   * @throws IOException if there was an IO error adding the profile
   * @throws AlreadyExistsException if the profile already exists
   */
  public void add(final ProfileId profileId,
                  final Profile profile) throws IOException, AlreadyExistsException {
    Transactionals.execute(transactional, context -> {
      Table metaTable = getTable(context);
      // make sure that a profile doesn't exist
      byte[] principalBytes = metaTable.get(getRowKey(profileId), COL);
      if (principalBytes != null) {
        throw new AlreadyExistsException(profileId,
                                         String.format("Profile '%s' already exists.",
                                                       profile.getName()));
      }
      metaTable.put(getRowKey(profileId), COL, Bytes.toBytes(GSON.toJson(profile)));
    }, IOException.class, AlreadyExistsException.class);
  }

  /**
   * Deletes the profile from the profile store
   *
   * @param profileId the id of the profile to delete
   * @throws IOException if there was an IO error deleting the profile
   * @throws NotFoundException if the profile is not found
   */
  public void delete(final ProfileId profileId) throws IOException, NotFoundException {
    Transactionals.execute(transactional, context -> {
      byte[] value = getTable(context).get(getRowKey(profileId), COL);
      if (value == null) {
        throw new NotFoundException(profileId);
      }
      getTable(context).delete(getRowKey(profileId));
    }, IOException.class, NotFoundException.class);
  }

  private Scan scanProfiles(NamespaceId namespaceId) {
    byte[] startRow = Bytes.toBytes(String.format("%s:%s:", PROFILE_PREFIX, namespaceId.getNamespace()));
    return new Scan(startRow, Bytes.stopKeyForPrefix(startRow));
  }

  private Table getTable(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, DATASET_ID, Table.class.getName(),
                                           TABLE_PROPERTIES);
  }

  private static byte[] getRowKey(ProfileId profileId) {
    return Bytes.toBytes(Joiner.on(':').join(PROFILE_PREFIX, profileId.getNamespace(),
                                             profileId.getProfile()));
  }
}
