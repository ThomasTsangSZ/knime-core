
package org.knime.core.data.store;

import org.knime.core.data.store.table.value.ReadableVectorAccess;
import org.knime.core.data.store.table.value.WritableVectorAccess;

public interface VecAccessible extends AutoCloseable {

	WritableVectorAccess getWriteAccess();

	ReadableVectorAccess createReadAccess();
}
