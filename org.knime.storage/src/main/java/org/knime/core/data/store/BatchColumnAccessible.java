package org.knime.core.data.store;

public interface BatchColumnAccessible extends AutoCloseable {

	BatchColumnReadAccess readAccess();

	BatchColumnWriteAccess writeAccess();

}
