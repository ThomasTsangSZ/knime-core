package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.store.BatchSpec;
import org.knime.core.data.store.Store;

// TODO maybe we want to actually split into read/write later.
public class ArrowStore implements Store<ArrowInMemoryBatch> {

	private ArrowColumnAccessibleFactory m_factory;
	private BatchSpec m_spec;
	private File m_dest;
	private RootAllocator m_root;

	public ArrowStore(File dest, final BatchSpec spec, final long limit) {
		// TODO add allocation listener for this store.
		// TODO likely we need one central allocator for ALL tables/stores
		m_root = new RootAllocator(limit);
		m_factory = new ArrowColumnAccessibleFactory(2048, m_root);
		m_spec = spec;
		m_dest = dest;
	}

	@Override
	public void persist(ArrowInMemoryBatch batch) {
		// TODO write to disc. 
		// TODO one file per batch. maybe physical batch size != logical (CPU cache etc)
	}

	@Override
	public ArrowInMemoryBatch load(long idx) {
		// TODO load from disc
		return null;
	}

	@Override
	public ArrowInMemoryBatch createNext() {
		// To be closed by caller
		return new ArrowInMemoryBatch(m_factory, m_spec);
	}

	@Override
	public void destroy() {
		// Assumption: we never, ever keep any references on batches, so all references
		// should have been closed externally.
		// however as a fallback we could simply kill the root allocator for this store.
		// TODO check if this is correct, in case someone is still "accessing the batch"
//		m_root.close();
		try {
			Files.delete(m_dest.toPath());
		} catch (IOException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

}
