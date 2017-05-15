/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb;

import com.mysql.clusterj.LockMode;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.TablesDef;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.*;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.wrapper.HopsTransaction;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.quota.*;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;

/**
 * A clusterj zoneConnector allows the DAL to connect to the backend NDB and MySQL databases.
 * The ClusterjConnector instance embeds a {@link MysqlServerConnector}
 */
public class ClusterjConnector implements StorageConnector {
  // logger
  private static final Log LOG = LogFactory.getLog(ClusterjConnector.class);

  // initialization data
  private final String clusterConnectString;
  private final String databaseName;

  private final HopsSession session;
  private final MysqlServerConnector mysql;

  private boolean tainted;

  /**
   * Initializes a clusterj connector.
   *
   * @param clusterConnectString
   * @param databaseName
   * @param session
   * @param mysql
   */
  protected ClusterjConnector(
      final String clusterConnectString,
      final String databaseName,
      final HopsSession session,
      final MysqlServerConnector mysql
  ) {
    this.clusterConnectString = clusterConnectString;
    this.databaseName = databaseName;
    this.session = session;
    this.mysql = mysql;
    // the connector is initially not tainted
    this.tainted = false;
  }

  /**
   * Taints the current connector.
   * The next thread requesting one, will get a new one.
   */
  private void taint() {
    this.tainted = true;
  }

  public boolean isTainted() {
    return this.tainted;
  }

  public MysqlServerConnector getMysqlConnector() {
    return this.mysql;
  }

  public void close() throws StorageException {
    this.session.close();
  }

  public HopsSession obtainSession() throws StorageException {
    return this.session;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    this.session.close();
  }

  /**
   * Begins a transaction.
   *
   * @throws StorageException
   */
  @Override
  public void beginTransaction() throws StorageException {
    HopsTransaction tx = session.currentTransaction();
    if (tx.isActive()) {
      throw new StorageException("Can not start Tx inside another Tx");
    }
    tx.begin();
  }

  /**
   * Fetches the active transaction from the session.
   *
   * @return the active transaction
   * @throws StorageException if there is no active transaction
   */
  private HopsTransaction activeTransaction() throws StorageException {
    HopsTransaction tx = session.currentTransaction();
    if (!tx.isActive()) {
      throw new StorageException("there is no active transaction");
    }
    return tx;
  }

  /**
   * Commits the active transaction.
   *
   * @throws StorageException if there are errors committing or if there is no active transaction
   */
  @Override
  public void commit() throws StorageException {
    try {
      activeTransaction().commit();
    } catch (StorageException exc) {
      this.taint();
      throw exc;
    }
  }

  /**
   * It rolls back only when the transaction is active.
   *
   * @throws StorageException if there are errors rolling back or if there is no active transaction
   */
  @Override
  public void rollback() throws StorageException {
    try {
      activeTransaction().rollback();
    } catch (StorageException exc) {
      this.taint();
      throw exc;
    }
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() throws StorageException {
    return formatAll(true);
  }

  @Override
  public boolean formatYarnStorage() throws StorageException {
    return formatYarn(true);
  }

  @Override
  public boolean formatHDFSStorage() throws StorageException {
    return formatHDFS(true);
  }

  @Override
  public boolean formatStorage(Class<? extends EntityDataAccess>... das)
      throws StorageException {
    return format(true, das);
  }


  @Override
  public boolean isTransactionActive() throws StorageException {
    return this.session.currentTransaction().isActive();
  }

  @Override
  public void readLock() throws StorageException {
    session.setLockMode(LockMode.SHARED);
  }

  @Override
  public void writeLock() throws StorageException {
    session.setLockMode(LockMode.EXCLUSIVE);
  }

  @Override
  public void readCommitted() throws StorageException {
    session.setLockMode(LockMode.READ_COMMITTED);
  }

  @Override
  public void setPartitionKey(Class className, Object key)
      throws StorageException {
    Class cls = null;
    if (className == BlockInfoDataAccess.class) {
      cls = BlockInfoClusterj.BlockInfoDTO.class;
    } else if (className == PendingBlockDataAccess.class) {
      cls = PendingBlockClusterj.PendingBlockDTO.class;
    } else if (className == ReplicaUnderConstructionDataAccess.class) {
      cls = ReplicaUnderConstructionClusterj.ReplicaUcDTO.class;
    } else if (className == INodeDataAccess.class) {
      cls = INodeClusterj.InodeDTO.class;
    } else if (className == INodeAttributesDataAccess.class) {
      cls = INodeAttributesClusterj.INodeAttributesDTO.class;
    } else if (className == LeaseDataAccess.class) {
      cls = LeaseClusterj.LeaseDTO.class;
    } else if (className == LeasePathDataAccess.class) {
      cls = LeasePathClusterj.LeasePathsDTO.class;
    } else if (className == HdfsLeDescriptorDataAccess.class) {
      cls = HdfsLeaderClusterj.HdfsLeaderDTO.class;
    } else if (className == YarnLeDescriptorDataAccess.class) {
      cls = YarnLeaderClusterj.YarnLeaderDTO.class;
    } else if (className == ReplicaDataAccess.class) {
      cls = ReplicaClusterj.ReplicaDTO.class;
    } else if (className == CorruptReplicaDataAccess.class) {
      cls = CorruptReplicaClusterj.CorruptReplicaDTO.class;
    } else if (className == ExcessReplicaDataAccess.class) {
      cls = ExcessReplicaClusterj.ExcessReplicaDTO.class;
    } else if (className == InvalidateBlockDataAccess.class) {
      cls = InvalidatedBlockClusterj.InvalidateBlocksDTO.class;
    } else if (className == UnderReplicatedBlockDataAccess.class) {
      cls = UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO.class;
    } else if (className == VariableDataAccess.class) {
      cls = VariableClusterj.VariableDTO.class;
    } else if (className == QuotaUpdateDataAccess.class) {
      cls = QuotaUpdateClusterj.QuotaUpdateDTO.class;
    } else if (className == EncodingStatusDataAccess.class) {
      cls = EncodingStatusClusterj.EncodingStatusDto.class;
    } else if (className == BlockChecksumDataAccess.class) {
      cls = BlockChecksumClusterj.BlockChecksumDto.class;
    } else if (className == OngoingSubTreeOpsDataAccess.class) {
      cls = OnGoingSubTreeOpsClusterj.OnGoingSubTreeOpsDTO.class;
    }

    session.setPartitionKey(cls, key);
    session.flush();
  }

  @Override
  public boolean formatAllStorageNonTransactional() throws StorageException {
    return formatAll(false);
  }

  @Override
  public boolean formatYarnStorageNonTransactional() throws StorageException {
    return formatAll(false);
  }

  @Override
  public boolean formatHDFSStorageNonTransactional() throws StorageException {
    return formatHDFS(false);
  }

  private boolean formatYarn(boolean transactional) throws StorageException {
    return format(transactional,
        ContainerIdToCleanDataAccess.class, ContainerStatusDataAccess.class,
        RMNodeDataAccess.class, FinishedApplicationsDataAccess.class,
        UpdatedContainerInfoDataAccess.class, YarnLeDescriptorDataAccess.class,
        RMLoadDataAccess.class, PendingEventDataAccess.class,
        NextHeartbeatDataAccess.class, ApplicationStateDataAccess.class,
        ApplicationAttemptStateDataAccess.class, DelegationKeyDataAccess.class,
        DelegationTokenDataAccess.class, ProjectQuotaDataAccess.class,
        ContainersLogsDataAccess.class, ContainersCheckPointsDataAccess.class,
        ProjectsDailyCostDataAccess.class, PriceMultiplicatorDataAccess.class);
  }

  private boolean formatHDFS(boolean transactional) throws StorageException {
    return format(transactional,
        INodeDataAccess.class, BlockInfoDataAccess.class, LeaseDataAccess.class,
        LeasePathDataAccess.class, ReplicaDataAccess.class,
        ReplicaUnderConstructionDataAccess.class,
        InvalidateBlockDataAccess.class, ExcessReplicaDataAccess.class,
        PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
        UnderReplicatedBlockDataAccess.class, HdfsLeDescriptorDataAccess.class,
        INodeAttributesDataAccess.class, StorageIdMapDataAccess.class,
        BlockLookUpDataAccess.class, SafeBlocksDataAccess.class,
        MisReplicatedRangeQueueDataAccess.class, QuotaUpdateDataAccess.class,
        EncodingStatusDataAccess.class, BlockChecksumDataAccess.class,
        OngoingSubTreeOpsDataAccess.class,
        MetadataLogDataAccess.class, AccessTimeLogDataAccess.class,
        SizeLogDataAccess.class, EncodingJobsDataAccess.class,
        RepairJobsDataAccess.class, UserDataAccess.class, GroupDataAccess.class,
        UserGroupDataAccess.class, VariableDataAccess.class);
  }

  private boolean formatAll(boolean transactional) throws StorageException {
    //HDFS
    if (!formatHDFS(transactional)) {
      return false;
    }
    //YARN
    if (!formatYarn(transactional)) {
      return false;
    }

    // shared
    return format(transactional,
        VariableDataAccess.class
    );
  }

  private boolean format(boolean transactional,
                         Class<? extends EntityDataAccess>... das) throws StorageException {

    final int RETRIES = 5; // in test
    for (int i = 0; i < RETRIES; i++) {
      try {
        for (Class e : das) {
          if (e == INodeDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.INodeTableDef.TABLE_NAME);
          } else if (e == BlockInfoDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockInfoTableDef.TABLE_NAME);
          } else if (e == LeaseDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.LeaseTableDef.TABLE_NAME);
          } else if (e == LeasePathDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.LeasePathTableDef.TABLE_NAME);
          } else if (e == OngoingSubTreeOpsDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.OnGoingSubTreeOpsDef.TABLE_NAME);
          } else if (e == ReplicaDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.ReplicaTableDef.TABLE_NAME);
          } else if (e == ReplicaUnderConstructionDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.ReplicaUnderConstructionTableDef.TABLE_NAME);
          } else if (e == InvalidateBlockDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.InvalidatedBlockTableDef.TABLE_NAME);
          } else if (e == ExcessReplicaDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.ExcessReplicaTableDef.TABLE_NAME);
          } else if (e == PendingBlockDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.PendingBlockTableDef.TABLE_NAME);
          } else if (e == CorruptReplicaDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.CorruptReplicaTableDef.TABLE_NAME);
          } else if (e == UnderReplicatedBlockDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.UnderReplicatedBlockTableDef.TABLE_NAME);
          } else if (e == HdfsLeDescriptorDataAccess.class) {
            this.mysql
                .truncateTable(transactional, TablesDef.HdfsLeaderTableDef.TABLE_NAME);
          } else if (e == INodeAttributesDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.INodeAttributesTableDef.TABLE_NAME);
          } else if (e == VariableDataAccess.class) {
            session.currentTransaction().begin();
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (Variable.Finder varType : Variable.Finder.values()) {
              VariableClusterj.VariableDTO vd =
                  session.newInstance(VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
            session.currentTransaction().commit();
          } else if (e == StorageIdMapDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.StorageIdMapTableDef.TABLE_NAME);
          } else if (e == BlockLookUpDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockLookUpTableDef.TABLE_NAME);
          } else if (e == SafeBlocksDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.SafeBlocksTableDef.TABLE_NAME);
          } else if (e == MisReplicatedRangeQueueDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MisReplicatedRangeQueueTableDef.TABLE_NAME);
          } else if (e == QuotaUpdateDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.QuotaUpdateTableDef.TABLE_NAME);
          } else if (e == EncodingStatusDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.EncodingStatusTableDef.TABLE_NAME);
          } else if (e == BlockChecksumDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockChecksumTableDef.TABLE_NAME);
          } else if (e == MetadataLogDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MetadataLogTableDef.TABLE_NAME);
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MetadataLogTableDef.LOOKUP_TABLE_NAME);
          } else if (e == AccessTimeLogDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.AccessTimeLogTableDef.TABLE_NAME);
          } else if (e == SizeLogDataAccess.class) {
            this.mysql.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.SizeLogTableDef.TABLE_NAME);
          } else if (e == EncodingJobsDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.EncodingJobsTableDef.TABLE_NAME);
          } else if (e == RepairJobsDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.RepairJobsTableDef.TABLE_NAME);
          } else if (e == UserDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.UsersTableDef.TABLE_NAME);
          } else if (e == GroupDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.GroupsTableDef.TABLE_NAME);
          } else if (e == UserGroupDataAccess.class) {
            this.mysql
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.UsersGroupsTableDef.TABLE_NAME);
          } else if (e == YarnLeDescriptorDataAccess.class) {
            this.mysql
                .truncateTable(transactional,
                    TablesDef.YarnLeaderTableDef.TABLE_NAME);
          } else if (e == ContainerIdToCleanDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ContainerIdToCleanTableDef.TABLE_NAME);
          } else if (e == ApplicationAttemptStateDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ApplicationAttemptStateTableDef.TABLE_NAME);
          } else if (e == ApplicationStateDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ApplicationStateTableDef.TABLE_NAME);
          } else if (e == DelegationKeyDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.DelegationKeyTableDef.TABLE_NAME);
          } else if (e == DelegationTokenDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.DelegationTokenTableDef.TABLE_NAME);
          } else if (e == ContainerStatusDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ContainerStatusTableDef.TABLE_NAME);
          } else if (e == ResourceDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ResourceTableDef.TABLE_NAME);
          } else if (e == RMNodeDataAccess.class) {
            // Truncate does not work with foreign keys
            truncate(true,
                io.hops.metadata.yarn.TablesDef.RMNodeTableDef.TABLE_NAME);
          } else if (e == FinishedApplicationsDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FinishedApplicationsTableDef.TABLE_NAME);
          } else if (e == UpdatedContainerInfoDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.UpdatedContainerInfoTableDef.TABLE_NAME);
          } else if (e == RMLoadDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RMLoadTableDef.TABLE_NAME);
          } else if (e == PendingEventDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.PendingEventTableDef.TABLE_NAME);
          } else if (e == NextHeartbeatDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.NextHeartbeatTableDef.TABLE_NAME);
          } else if (e == ProjectQuotaDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ProjectQuotaTableDef.TABLE_NAME);
          } else if (e == ContainersLogsDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ContainersLogsTableDef.TABLE_NAME);
          } else if (e == ContainersCheckPointsDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ContainersCheckPointsTableDef.TABLE_NAME);
          } else if (e == ProjectsDailyCostDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ProjectsDailyCostTableDef.TABLE_NAME);
          } else if (e == PriceMultiplicatorDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.PriceMultiplicatorTableDef.TABLE_NAME);
          }
        }
        this.mysql.truncateTable(transactional,
            "hdfs_path_memcached");
        return true;

      } catch (SQLException ex) {
        LOG.error(ex.getMessage(), ex);
      }
    } // end retry loop
    return false;
  }

  private void truncate(boolean transactional, String tableName)
      throws StorageException, SQLException {
    this.mysql.truncateTable(transactional, tableName);
  }

  @Override
  public void dropAndRecreateDB() throws StorageException {
    this.mysql.dropAndRecreateDB();
  }

  @Override
  public void flush() throws StorageException {
    this.session.flush();
  }

  public String getClusterConnectString() {
    return clusterConnectString;
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
