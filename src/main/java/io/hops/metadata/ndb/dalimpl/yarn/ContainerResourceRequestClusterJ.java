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
package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import static io.hops.metadata.yarn.TablesDef.ContainerResourceRequestTableDef.NAME;
import static io.hops.metadata.yarn.TablesDef.ContainerResourceRequestTableDef.RESOURCEREQUESTSTATE;
import static io.hops.metadata.yarn.TablesDef.ContainerResourceRequestTableDef.TABLE_NAME;
import static io.hops.metadata.yarn.TablesDef.ContainerResourceRequestTableDef.CONTAINER_ID;
import io.hops.metadata.yarn.dal.ContainerResourceRequestDataAccess;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

public class ContainerResourceRequestClusterJ implements
    TablesDef.ContainerResourceRequestTableDef,
    ContainerResourceRequestDataAccess<ResourceRequest> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerResourceRequestDTO {
      
    @PrimaryKey
    @Column(name = CONTAINER_ID)
    String getContainerId();
    
    String setContainerId(String container_id);
    
    @PrimaryKey
    @Column(name = NAME)
    String getName();
    
    void setName(String name);

    @Column(name = RESOURCEREQUESTSTATE)
    byte[] getResourceRequestState();

    void setResourceRequestState(byte[] resourceRequestState);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();


  @Override
  public Map<String, List<ResourceRequest>> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ContainerResourceRequestDTO> dobj =
        qb.createQueryDefinition(ContainerResourceRequestDTO.class);
    HopsQuery<ContainerResourceRequestDTO> query = session.
        createQuery(dobj);
    List<ContainerResourceRequestDTO> queryResults = query.
        getResultList();
    Map<String,List<ResourceRequest>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<ResourceRequest> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerResourceRequestDTO> toPersist = new ArrayList<ContainerResourceRequestDTO>();
    for (ResourceRequest req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<ResourceRequest> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerResourceRequestDTO> toPersist = new ArrayList<ContainerResourceRequestDTO>();
    for (ResourceRequest hop : toRemove) {
      Object[] pk = new Object[2];
      pk[0] = hop.getContainerId();
      pk[1] = hop.getName();
      toPersist.add(session.newInstance(ContainerResourceRequestDTO.class, pk));
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  private ResourceRequest createHopContainerResourceRequest(
      ContainerResourceRequestDTO resourceRequestDTO) throws StorageException {
    try {
      return new ResourceRequest(resourceRequestDTO.getContainerId(),
              resourceRequestDTO.getName(),
              CompressionUtils.decompress(resourceRequestDTO.
                      getResourceRequestState()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private ContainerResourceRequestDTO createPersistable(ResourceRequest hop,
      HopsSession session) throws StorageException {
    ContainerResourceRequestClusterJ.ContainerResourceRequestDTO resourceRequestDTO = session.
        newInstance(ContainerResourceRequestClusterJ.ContainerResourceRequestDTO.class);

    resourceRequestDTO.setName(hop.getName());
    resourceRequestDTO.setContainerId(hop.getContainerId());
    try {
      resourceRequestDTO.setResourceRequestState(CompressionUtils.compress(hop.
          getResourcerequeststate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    return resourceRequestDTO;
  }

  private Map<String, List<ResourceRequest>> createMap(
      List<ContainerResourceRequestDTO> results) throws StorageException {
    Map<String, List<ResourceRequest>> map =
        new HashMap<String, List<ResourceRequest>>();
    for (ContainerResourceRequestDTO dto : results) {
      ResourceRequest hop = createHopContainerResourceRequest(dto);
      if (map.get(hop.getContainerId()) == null) {
        map.put(hop.getContainerId(), new ArrayList<ResourceRequest>());
      }
      map.get(hop.getContainerId()).add(hop);
    }
    return map;
  }
}
