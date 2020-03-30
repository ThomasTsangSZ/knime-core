
package org.knime.core.data.store.vec;

import org.knime.core.data.store.vec.rw.ReadableVectorAccess;
import org.knime.core.data.store.vec.rw.WritableVectorAccess;

public interface VecAccessible extends AutoCloseable {

	WritableVectorAccess getWriteAccess();

	ReadableVectorAccess createReadAccess();
}
