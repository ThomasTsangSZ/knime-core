package org.knime.core.data.store.vec;

public interface Vec extends AutoCloseable {

	VecReadAccess readAccess();

	VecWriteAccess writeAccess();

}
