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

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.query.QueryDefinition;
import io.hops.exception.StorageException;

public class HopsQueryDefinition<E> {
  private final QueryDefinition<E> queryDefinition;
  private final HopsExceptionWrapper wrapper;

  public HopsQueryDefinition(final HopsExceptionWrapper wrapper, final QueryDefinition<E> queryDefinition) {
    this.wrapper = wrapper;
    this.queryDefinition = queryDefinition;
  }

  public HopsQueryDefinition<E> where(HopsPredicate predicate)
      throws StorageException {
    try {
      return new HopsQueryDefinition<>(
          this.wrapper,
          queryDefinition.where(predicate.getPredicate()));
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public HopsPredicateOperand param(String s) throws StorageException {
    try {
      return new HopsPredicateOperand(
          this.wrapper,
          queryDefinition.param(s));
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }

  public HopsPredicate not(HopsPredicate predicate) throws StorageException {
    try {
      return new HopsPredicate(
          this.wrapper,
          queryDefinition.not(predicate.getPredicate()));
    } catch (ClusterJException e) {
      throw wrapper.toStorageException(e);
    }
  }
}
