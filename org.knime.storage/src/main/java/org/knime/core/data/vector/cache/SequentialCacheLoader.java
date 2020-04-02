
package org.knime.core.data.vector.cache;

import java.io.IOException;

import org.knime.core.data.vector.RefManaged;

public interface SequentialCacheLoader<O extends RefManaged> {

	O load(long index) throws IOException;
}
