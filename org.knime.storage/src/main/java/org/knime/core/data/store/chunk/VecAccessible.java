
package org.knime.core.data.store.chunk;

public interface VecAccessible extends AutoCloseable {

	WritableVectorAccess getWriteAccess();

	ReadableVectorAccess createReadAccess();
}
