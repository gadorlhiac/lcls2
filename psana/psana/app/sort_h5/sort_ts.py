from mpi4py import MPI
import os, sys
import time
import h5py
import numpy as np
import dask
import dask.array as da
import dask.dataframe as dd

from utils import get_dask_client

class TsSort:
    def __init__(self, in_h5, out_h5,
            chunk_size,
            n_procs,
            n_jobs,
            batch_size,
            n_ranks):
        self.in_h5fname = in_h5
        self.out_h5fname= out_h5
        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.n_ranks    = n_ranks
        self.n_procs    = n_procs
        self.n_jobs     = n_jobs

    def sort(self):
        client = get_dask_client(self.n_procs, n_jobs=self.n_jobs) 
        ts_chunks = (self.chunk_size, )
        in_f = h5py.File(self.in_h5fname, 'r')
        da_ts = da.from_array(in_f['timestamp'], chunks=ts_chunks)
        dd_ts = dd.from_array(da_ts, columns=['timestamp'])

        # Sorting (sort_values is not in-place)
        dd_ts_sorted = dd_ts.sort_values('timestamp')

        # Load indices
        inds = dd_ts_sorted.index.values
        inds_arr = np.asarray(inds.compute(), dtype=np.int64)
        in_f.close()
        return inds_arr

    def slice_and_write(self, inds_arr):
        # Spawn mpiworkers
        maxprocs = self.n_ranks
        sub_comm = MPI.COMM_SELF.Spawn(sys.executable, args=['parallel_h5_write.py'], maxprocs=maxprocs,)
        common_comm=sub_comm.Merge(False)

        # Send data
        n_samples = inds_arr.shape[0]
        batch_size = self.batch_size
        n_files = int(np.ceil(n_samples/batch_size))
        rankreq = np.empty(1, dtype='i')
        for i in range(n_files):
            st = i * batch_size
            en = st + batch_size
            if en > n_samples: en = n_samples
            common_comm.Recv(rankreq, source=MPI.ANY_SOURCE)
            common_comm.Send(inds_arr[st:en], tag=i, dest=rankreq[0])
            print(f'MAIN: Sent {st}:{en} part:{i} to writer {rankreq[0]}')

        # Kill clients
        for i in range(common_comm.Get_size()-1):
            common_comm.Recv(rankreq, source=MPI.ANY_SOURCE)
            common_comm.Send(bytearray(), dest=rankreq[0])
        commom_comm.Barrier()
        common_comm.Abort(1)

    def view(self, n_rows=10):
        # Check the first n_rows timestamps
        chk_f = h5py.File(self.out_h5fname, 'r')
        print(f"MAIN: {chk_f['timestamp'][:n_rows]}")
        chk_f.close()
