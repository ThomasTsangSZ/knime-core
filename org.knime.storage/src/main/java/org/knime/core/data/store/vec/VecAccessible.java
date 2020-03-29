
package org.knime.core.data.store.vec;

import org.knime.core.data.store.vec.rw.VecReadAccess;
import org.knime.core.data.store.vec.rw.VecWriteAccess;

public interface VecAccessible extends AutoCloseable {

	VecWriteAccess getWriteAccess();

	VecReadAccess createReadAccess();
}
