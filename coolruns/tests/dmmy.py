
import gc


import numpy as np
import pandas as pd

import coolruns as cr
import coolruns.iterops as iterops

from coolruns.tests.sampler import fastdata, mixdata



if __name__ == "__main__":

    from datetime import datetime
    otim = lambda dlta:int(((dlta.seconds*1E+6)+dlta.microseconds)//1e+3)
    mark = datetime.now()
    pass
    dlen = 1024*1024*100
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | Start")
    pass
    data = pd.DataFrame(**fastdata(dlen=dlen, edat="2025-07-01", datyp=np.double))
    #data = pd.DataFrame(**fastdata(dlen=dlen, edat="2025-07-01", datyp=np.half))
    #data = pd.DataFrame(**fastdata(dlen=dlen, edat="2025-07-01", datyp=np.half))
    #data = pd.DataFrame(**mixdata(dlen=dlen//10, edat="2025-07-01", datyp=np.half))
    pass
    data.coolruns
    pass
    data.coolruns(64, "dimm64")(32, "dimm32")(16, "dimm16")
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | ForLoop")
    pass
    floo = [dimm for dimm in data.coolruns]
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | IterOps")
    pass
    """
    wrto = RunOper()(iterops.WindowRatio(data.coolruns.wdat, blen=max(64, dlen//(1024*10))), feld=data.coolruns.feld)
    aprs = RunOper(maxw=10)(iterops.AutoPearson(data.coolruns["dimm64"].wdat[...,0], blen=data.coolruns["dimm64"].shape[1]))
    wrto = RunOper()(iterops.WindowRatio(data.coolruns.wdat[...,0], blen=max(64, dlen//(1024*10))))
    """
    pass
    data.coolruns.iterop(iterops.WindowRatio, blen=max(64, dlen//(1024*10)))
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | Finish")
    print(data.coolruns.shape)
    gc.collect()
    breakpoint()
    pass
    """
    # Find repeated values in wrto.data column 0
    codes, uniques = pd.Series(wrto.data[:,0].ravel()).factorize()
    #codes, uniques = pd.Series(wrto.data.ravel()).factorize()
    pass
    values, counts = np.unique(wrto.data, return_counts=True)
    repeated = values[counts > 1]
    #print("Repeated values in wrto.data:", repeated)
    """
    import time
    while True:
        time.sleep(2)
        pass