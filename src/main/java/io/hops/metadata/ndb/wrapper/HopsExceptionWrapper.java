package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import io.hops.exception.*;

/**
 * This class converts ClusterJExceptions into the proper type of StorageException.
 */
public class HopsExceptionWrapper {
  public StorageException toStorageException(final ClusterJException exc) {
    // we can't assume anything, so we throw a generic (non-transient) exception
    if (!(exc instanceof ClusterJDatastoreException)) {
      return new StorageException(exc);
    }

    ClusterJDatastoreException dsExc = (ClusterJDatastoreException) exc;
    if(isTransient(dsExc)){
      return new TransientStorageException(dsExc);
    } else if (isTupleAlreadyExisted(dsExc)) {
      return new TupleAlreadyExistedException(dsExc);
    } else if (isForeignKeyConstraintViolation(dsExc)) {
      return new ForeignKeyConstraintViolationException(dsExc);
    } else if (isUniqueKeyConstraintViolation(dsExc)) {
      return new UniqueKeyConstraintViolationException(dsExc);
    } else if (isClusterFailure(dsExc)) {
      return new ClusterFailureException(dsExc);
    }

    // we don't know what this is...
    return new StorageException(dsExc);
  }


  private boolean isTransient(ClusterJDatastoreException e) {
      // http://dev.mysql.com/doc/ndbapi/en/ndb-error-classifications.html
      // The classifications can be found in ndberror.h and ndberror.c in the ndb sources
      return e.getClassification() == 7 || // Temporary Resource error (TR)
          e.getClassification() == 8    || // Node Recovery error (NR)
          e.getClassification() == 9    || // Overload error (OL)
          e.getClassification() == 10   || // Timeout expired (TO)
          e.getClassification() == 15   || // Node shutdown (NS)
          e.getClassification() == 18;     // Internal temporary (IT)
  }

  private boolean isTupleAlreadyExisted(ClusterJDatastoreException e) {
    return e.getCode() == 630;
  }

  private boolean isForeignKeyConstraintViolation(ClusterJDatastoreException e) {
    return e.getCode() == 255;
  }

  private boolean isUniqueKeyConstraintViolation(ClusterJDatastoreException e) {
    return e.getCode() == 893;
  }

  protected boolean isClusterFailure(ClusterJDatastoreException dsExc) {
    return dsExc.getCode() == 4009;
  }
}
