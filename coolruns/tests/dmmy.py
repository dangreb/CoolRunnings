

import coolruns

import numpy as np
import pandas as pd

from coolruns.oper import WindowRatio
from coolruns.tests.helpers import fastdata, mixdata


if __name__ == "__main__":
    from datetime import datetime
    otim = lambda dlta:int(((dlta.seconds*1E+6)+dlta.microseconds)//1e+3)
    mark = datetime.now()

    dlen = 1024*1024*10

    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | Start")

    data = pd.DataFrame(**fastdata(dlen=dlen, edat="2025-07-01", datyp=np.uint32, ))
    data.coolruns(16, "dimm1")(32, "dimm2")(64, "dimm3")

    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | IterOps")

    wrto = WindowRatio(data.coolruns, blen=64).RunOper(maxw=8)()

    print(f"{datetime.now()} | {otim(datetime.now()-mark)} | Finish")

    breakpoint()




    pass