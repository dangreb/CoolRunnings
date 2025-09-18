

import coolruns

import numpy as np
import pandas as pd

from coolruns.oper import WindowRatio
from coolruns.tests.helpers import fastdata, mixdata


if __name__ == "__main__":
    from datetime import datetime
    otim = lambda dlta:int(((dlta.seconds*1E+6)+dlta.microseconds)//1e+3)
    mark = datetime.now()
    pass
    dlen = 1024*1024*100
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | Start")
    pass
    data = pd.DataFrame(**fastdata(dlen=dlen, edat="2025-07-01", datyp=np.uint32, ))
    data.coolruns(64, "dimm64")(32, "dimm32")(16, "dimm16")
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | ForLoop")
    pass
    floo = [dimm for dimm in data.coolruns]
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | IterOps")
    pass
    #wrto = RunOper(maxw=8)(WindowRatio(data.coolruns, blen=max(64, dlen//1024)))
    wrto = WindowRatio(data.coolruns, blen=max(64, dlen//1024*100)).RunOper(maxw=8)
    pass
    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | Finish")
    breakpoint()




    pass