import numpy as np
import data_structs as ds

tran_dtype = np.dtype(
    [('note', np.str_, 100), ('symbol', np.str_, 10), ('trans_date', 'datetime64[D]'), ('settle_date', 'datetime64[D]'), ('acct_currency', np.str_, 6), ('type', np.str_, 15), ('qty', np.float32), ('price_currency', np.str_, 6), ('price', np.float32), ('amount', np.float32)])

class np_Time_Series(np.ndarray):
	def __init__(self, _data):
		# super(np_Time_Series, self).__init__(_data, dtype=tran_dtype)
		np.array.__init__(self, _data, dtype=tran_dtype)

ts = ds.TimeSeriesTrans()
print(type(ts))
np_ts = np_Time_Series(ts)
print(type(np_ts))


