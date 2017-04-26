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
package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.*;
import com.mysql.clusterj.query.QueryBuilder;
import io.hops.exception.StorageException;

import java.util.Collection;

public class HopsSession {
  private final Session session;
  private LockMode lockMode = LockMode.READ_COMMITTED;

  private final HopsExceptionWrapper wrapper;

  public HopsSession(final Session session, final HopsExceptionWrapper wrapper) {
    this.session = session;
    this.wrapper = wrapper;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    this.session.close();
  }

  public HopsQueryBuilder getQueryBuilder() throws StorageException {
    try {
      QueryBuilder queryBuilder = session.getQueryBuilder();
      return new HopsQueryBuilder(this.wrapper, queryBuilder);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> HopsQuery<T> createQuery(HopsQueryDomainType<T> queryDefinition)
      throws StorageException {
    try {
      Query<T> query = session.createQuery(queryDefinition.getQueryDomainType());
      return new HopsQuery<>(this.wrapper, query);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> T find(Class<T> aClass, Object o) throws StorageException {
    try {
      return session.find(aClass, o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> T newInstance(Class<T> aClass) throws StorageException {
    try {
      return session.newInstance(aClass);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> T newInstance(Class<T> aClass, Object o) throws StorageException {
    try {
      return session.newInstance(aClass, o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> T makePersistent(T t) throws StorageException {
    try {
      return session.makePersistent(t);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> T load(T t) throws StorageException {
    try {
      return session.load(t);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public Boolean found(Object o) throws StorageException {
    try {
      return session.found(o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void persist(Object o) throws StorageException {
    try {
      session.persist(o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public Iterable<?> makePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      return session.makePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> void deletePersistent(Class<T> aClass, Object o)
      throws StorageException {
    try {
      session.deletePersistent(aClass, o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void deletePersistent(Object o) throws StorageException {
    try {
      session.deletePersistent(o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void remove(Object o) throws StorageException {
    try {
      session.remove(o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> int deletePersistentAll(Class<T> aClass) throws StorageException {
    try {
      return session.deletePersistentAll(aClass);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void deletePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      session.deletePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void updatePersistent(Object o) throws StorageException {
    try {
      session.updatePersistent(o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void updatePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      session.updatePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> T savePersistent(T t) throws StorageException {
    try {
      return session.savePersistent(t);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public Iterable<?> savePersistentAll(Iterable<?> iterable)
      throws StorageException {
    try {
      return session.savePersistentAll(iterable);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public HopsTransaction currentTransaction() throws StorageException {
    try {
      Transaction transaction = session.currentTransaction();
      return new HopsTransaction(this.wrapper, transaction);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void close() throws StorageException {
    try {
      session.close();
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public boolean isClosed() throws StorageException {
    try {
      return session.isClosed();
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void flush() throws StorageException {
    try {
      session.flush();
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void setPartitionKey(Class<?> aClass, Object o)
      throws StorageException {
    try {
      session.setPartitionKey(aClass, o);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void setLockMode(LockMode lockMode) throws StorageException {
    try {
      session.setLockMode(lockMode);
      this.lockMode = lockMode;
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public void markModified(Object o, String s) throws StorageException {
    try {
      session.markModified(o, s);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public String unloadSchema(Class<?> aClass) throws StorageException {
    try {
      return session.unloadSchema(aClass);
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> void release(T t) throws StorageException {
    try {
      if (t != null) {
        session.release(t);
      }
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public <T> void release(Collection<T> t) throws StorageException {
    try {
      if (t != null) {
        for (T dto : t) {
          session.release(dto);
        }
      }
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public LockMode getCurrentLockMode() {
    return lockMode;
  }
}
