import sys, os, time, numpy as np, pyarrow as pa, pyarrow.parquet as pq
TOTAL=int(sys.argv[1]); OUT=sys.argv[2]; PER=int(sys.argv[3]); CHUNK=1_000_000
os.makedirs(OUT, exist_ok=True)
def D(vals, idx):  # dictionary-encoded string column
    return pa.DictionaryArray.from_arrays(pa.array(idx, type=pa.int8()), pa.array(vals))
V = dict(
 status=["completed","shipped","pending","cancelled","returned"],
 channel=["web","mobile","store","marketplace"],
 pay=["credit_card","debit_card","paypal","cod","bank_transfer","e_wallet"],
 cat=["electronics","fashion","home","beauty","sports","books","toys","grocery"],
 cur=["USD","EUR","VND","GBP","JPY"], country=["VN","US","DE","GB","JP","SG","FR","AU"],
 city=["Hanoi","HCMC","New York","Berlin","London","Tokyo","Singapore","Paris","Sydney","Da Nang"],
 gender=["M","F","O"], seg=["regular","premium","vip","new"], dev=["ios","android","desktop","tablet"],
 coupons=["SAVE10","WELCOME","FREESHIP","SUMMER25","VIP50"],
 notes=["gift wrap","leave at door","call before delivery","fragile","express request"])
base=np.datetime64("2023-01-01")
def make(start,n,rng):
    ri=lambda lo,hi:rng.integers(lo,hi,n)
    day=ri(0,1000); sec=ri(0,86400); qty=ri(1,11); price=np.round(rng.uniform(1,1000,n),2)
    disc=rng.choice(np.array([0,5,10,15,20,25]),n); sub=qty*price*(1-disc/100)
    tax=np.round(sub*0.08,2); ship=np.round(rng.uniform(0,30,n),2); dd=ri(1,15)
    od=base+day.astype("timedelta64[D]")
    ts=(od.astype("datetime64[s]")+sec.astype("timedelta64[s]"))
    nulls=lambda p:rng.random(n)<p
    cols={
     "order_id":pa.array(np.arange(start+1,start+n+1,dtype=np.int64)),
     "customer_id":pa.array(ri(1,2_000_001).astype(np.int32)),
     "product_id":pa.array(ri(1,100_001).astype(np.int32)),
     "order_date":pa.array(od.astype("datetime64[D]")),
     "order_timestamp":pa.array(ts.astype("datetime64[s]")).cast(pa.timestamp("us")),
     "ship_date":pa.array((od+dd.astype("timedelta64[D]")).astype("datetime64[D]")),
     "status":D(V["status"],rng.choice(5,n,p=[.6,.15,.1,.08,.07])),
     "channel":D(V["channel"],ri(0,4)),"payment_method":D(V["pay"],ri(0,6)),
     "category":D(V["cat"],ri(0,8)),
     "quantity":pa.array(qty.astype(np.int32)),"unit_price":pa.array(price),
     "discount_pct":pa.array(disc.astype(np.int32)),"tax_amount":pa.array(tax),
     "shipping_fee":pa.array(ship),"total_amount":pa.array(np.round(sub+tax+ship,2)),
     "currency":D(V["cur"],ri(0,5)),"country":D(V["country"],ri(0,8)),"city":D(V["city"],ri(0,10)),
     "postal_code":pa.array(ri(10000,99999).astype(np.int32)),
     "customer_age":pa.array(ri(18,80).astype(np.int32)),
     "gender":D(V["gender"],ri(0,3)),"customer_segment":D(V["seg"],ri(0,4)),
     "is_member":pa.array(rng.random(n)<0.4),
     "rating":pa.array(ri(1,6).astype(np.int32),mask=nulls(0.2)),
     "delivery_days":pa.array(dd.astype(np.int32)),
     "return_flag":pa.array(rng.random(n)<0.07),
     "device_type":D(V["dev"],ri(0,4)),
     "coupon_code":pa.DictionaryArray.from_arrays(pa.array(ri(0,5).astype(np.int8),mask=nulls(0.7)),pa.array(V["coupons"])),
     "notes":pa.DictionaryArray.from_arrays(pa.array(ri(0,5).astype(np.int8),mask=nulls(0.85)),pa.array(V["notes"])),
    }
    return pa.table(cols)
t0=time.time(); done=0; f=0
while done<TOTAL:
    fn=min(PER,TOTAL-done); path=f"{OUT}/orders_part_{f:04d}.parquet"; w=None
    for s in range(0,fn,CHUNK):
        n=min(CHUNK,fn-s); t=make(done+s,n,np.random.default_rng([42,(done+s)//CHUNK]))
        if w is None: w=pq.ParquetWriter(path,t.schema,compression="zstd",compression_level=3,use_dictionary=True)
        w.write_table(t,row_group_size=CHUNK)
    w.close(); done+=fn; f+=1
    print(path,f"{done:,}",f"{time.time()-t0:.0f}s",f"{os.path.getsize(path)/1e6:.0f}MB",flush=True)
