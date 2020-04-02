
package org.knime.core.data.store.arrow;

import com.google.common.io.Files;

import io.netty.buffer.ArrowBuf;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.arrow.therealstore.ArrowTableStore;
import org.knime.core.data.store.column.ColumnSchema;

public final class ArrowUtils {

	private ArrowUtils() {}

	public static void retainVector(final ValueVector vector) {
		for (final ArrowBuf buffer : vector.getBuffers(false)) {
			buffer.getReferenceManager().retain();
		}
	}

	public static void releaseVector(final ValueVector vector) {
		for (final ArrowBuf buffer : vector.getBuffers(false)) {
			buffer.getReferenceManager().release();
		}
	}

	public static ArrowTable createArrowTable(final int batchSize, final long offHeapSize, final ColumnSchema... schemas)
		throws IOException
	{
		final File baseDirectory = Files.createTempDir();
		baseDirectory.deleteOnExit();
		final ArrowTableStore store = new ArrowTableStore(baseDirectory, schemas, batchSize); // TODO
		return new ArrowTable(schemas, store);
	}
}
