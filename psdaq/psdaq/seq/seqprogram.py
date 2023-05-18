import time
from psdaq.seq.seq import *
from psdaq.cas.pvedit import *
from threading import Lock
import argparse


class SeqUser:
    def __init__(self, base):
        prefix = base
        self.ninstr   = Pv(prefix+':INSTRCNT')
        self.desc     = Pv(prefix+':DESCINSTRS')
        self.instr    = Pv(prefix+':INSTRS')
        self.idxseq   = Pv(prefix+':SEQ00IDX')
        self.seqname  = Pv(prefix+':SEQ00DESC')
        self.seqbname = Pv(prefix+':SEQ00BDESC')
        self.idxseqr  = Pv(prefix+':RMVIDX')
        self.seqr     = Pv(prefix+':RMVSEQ')
        self.insert   = Pv(prefix+':INS')
        self.idxrun   = Pv(prefix+':RUNIDX')
        self.start    = Pv(prefix+':SCHEDRESET')
        self.reset    = Pv(prefix+':FORCERESET')
        self.running  = Pv(prefix+':RUNNING', self.changed)
        self._idx     = 0
        self.lock     = None

    def changed(self,err=None):
        q = self.running.__value__
        if q==0 and self.lock!=None:
            self.lock.release()
            self.lock=None

    def stop(self):
        self.idxrun.put(0)  # a do-nothing sequence
        self.reset .put(1)
        self.reset .put(0)

    def clean(self):
        # Remove existing sub sequences
        ridx = -1
        print( 'Remove %d'%ridx)
        if ridx < 0:
            idx = self.idxseq.get()
            print(f'idx {idx}')
            while (idx>0):
                print( 'Removing seq %d'%idx)
                self.idxseqr.put(idx)
                self.seqr.put(1)
                self.seqr.put(0)
                time.sleep(1.0)
                idx = self.idxseq.get()
        elif ridx > 1:
            print( 'Removing seq %d'%ridx)
            self.idxseqr.put(ridx)
            self.seqr.put(1)
            self.seqr.put(0)

    def load(self, title, instrset, descset=None):
        self.desc.put(title)

        encoding = [len(instrset)]
        for instr in instrset:
            encoding = encoding + instr.encoding()

        print( encoding)

        self.instr.put( tuple(encoding) )

        time.sleep(1.0)

        ninstr = self.ninstr.get()
        if ninstr != len(instrset):
            print( 'Error: ninstr invalid %u (%u)' % (ninstr, len(instrset)))
            return

        print( 'Confirmed ninstr %d'%ninstr)

        self.insert.put(1)
        self.insert.put(0)

        #  How to handshake the insert.put -> idxseq.get (RPC?)
        time.sleep(1.0)

        #  Get the assigned sequence num
        idx = self.idxseq.get()
        if idx < 2:
            print( 'Error: subsequence index  invalid (%u)' % idx)
            raise RuntimeError("Sequence failed")

        print( 'Sequence '+self.seqname.get()+' found at index %d'%idx)

        #  (Optional for XPM) Write descriptions for each bit in the sequence
        if descset!=None:
            self.seqbname.put(descset)

        self._idx = idx

    def begin(self, wait=False):
        self.idxrun.put(self._idx)
        self.reset .put(0)
        self.start .put(1)
        self.start .put(0)
        if wait:
            self.lock= Lock()
            self.lock.acquire()

    def sync(self):
        self.idxrun.put(self._idx)
        self.reset .put(0)
        self.start .put(2)
        self.start .put(0)

    def execute(self, title, instrset, descset=None, sync=False):
        self.insert.put(0)
        self.stop ()
        self.clean()
        self.load (title,instrset,descset)
        if sync:
            self.sync()
        else:
            self.begin()


def main():
    parser = argparse.ArgumentParser(description='sequence pva programming')
    parser.add_argument('--pv', type=str, required=True, help="sequence engine pv; e.g. DAQ:NEH:XPM:0")
    parser.add_argument("--seq", required=True, nargs='+', type=str, help="sequence engine:script pairs; e.g. 0:train.py")
    parser.add_argument("--start", action='store_true', help="start the sequences")
    parser.add_argument("--verbose", action='store_true', help="verbose output")
    args = parser.parse_args()

    files = []
    engineMask = 0

    seqcodes_pv = Pv(f'{args.pv}:SEQCODES',isStruct=True)
    seqcodes = seqcodes_pv.get()
    desc = seqcodes.value.Description

    for s in args.seq:
        sengine,fname = s.split(':',1)
        engine = int(sengine)
        print(f'** engine {engine} fname {fname} **')

        config = {'title':'TITLE', 'descset':None, 'instrset':None, 'seqcodes':None}
        seq = 'from psdaq.seq.seq import *\n'
        seq += open(fname).read()
        exec(compile(seq, fname, 'exec'), {}, config)
        
        print(f'descset  {config["descset"]}')
        print(f'seqcodes {config["seqcodes"]}')
        if args.verbose:
            print('instrset:')
            for i in config["instrset"]:
                print(i)

        seq = SeqUser(f'{args.pv}:SEQENG:{engine}')
        seq.execute(config['title'],config['instrset'],config['descset'],sync=True)
        del seq

        engineMask |= (1<<engine)

        for e in range(4*engine,4*engine+4):
            desc[e] = ''
        for e,d in config['seqcodes'].items():
            desc[4*engine+e] = d
        print(f'desc {desc}')

    v = seqcodes.value
    v.Description = desc
    seqcodes.value = v

    print(f'seqcodes_pv {seqcodes}')
    seqcodes_pv.put(seqcodes)

    if args.start:
        pvSeqReset = Pv(f'{args.pv}:SeqReset')
        pvSeqReset.put(engineMask)
        

if __name__ == '__main__':
    main()

