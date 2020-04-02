
package org.knime.core.data.vector.cache;

import java.io.IOException;

import org.knime.core.data.vector.RefManaged;

public interface CacheFlusher<O extends RefManaged> {

	void flush(O obj) throws IOException;
}
