"""Wrapper chay tiep sinh du lieu 100M (khong sua run_generation.py / data_generator.py / params.yaml).

Lenh:
  python synthetic_bhxh/scripts/resume_100M.py dry-run   # in ke hoach + uoc luong, khong chay
  python synthetic_bhxh/scripts/resume_100M.py launch    # khoi chay dieu phoi tach roi khoi terminal/agent
  python synthetic_bhxh/scripts/resume_100M.py status    # in file trang thai
  python synthetic_bhxh/scripts/resume_100M.py stop      # dung an toan (chunk dang chay se hoan tat truoc)
  python synthetic_bhxh/scripts/resume_100M.py run       # noi bo (do launch goi)

Quy uoc:
  - Chunk 45..166: sinh day du, ghi 4 Parquet + file dap an loi (nguyen tu: ten tam -> doi ten).
  - Chunk 0..44: sinh lai trong bo nho, so bam tung cot voi file Parquet hien co (khong bao gio ghi de);
    chi ghi file dap an loi neu khop. Neu LECH: ghi log, KHONG ghi dap an loi cua chunk do.
  - Dau hoan tat = _resume_100M/done/chunk_XXXXX.json, tao sau cung.
"""
import argparse
import ctypes
import datetime
import hashlib
import io
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import time
import traceback
from ctypes import wintypes
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT / "synthetic_bhxh"))

SCALE = "100M"
TARGET_MASTERS = 8333333          # = scale_masters['100M'] trong run_generation.py
CHUNK = 50000                     # = chunk_size trong run_generation.py
NCHUNKS = (TARGET_MASTERS + CHUNK - 1) // CHUNK   # 167
N_EXISTING = 45                   # chunk 0..44 da co tren dia
TABLES = ["MASTER", "DETAIL", "ML_LABELS", "ML_ANOMALY"]

B = ROOT / "synthetic_bhxh"
OUT = B / "output" / SCALE
RES = B / "_resume_100M"
DONE = RES / "done"
CLAIMS = RES / "claims"
FAILED = RES / "failed"
KEYS_PARTS = B / "_answer_key" / "_parts_100M"
FINAL_KEY = B / "_answer_key" / "error_keys_100M.csv"
LOG = RES / "resume.log"
STATUS = RES / "status.json"
PIDF = RES / "coordinator.pid"
STOPF = RES / "STOP"
MANIFEST = RES / "manifest.json"
SNAPSHOT = RES / "existing_snapshot.json"
STDOUT_LOG = RES / "coordinator.stdout.log"

# So do o Buoc 2 + bo sung cua nguoi dung
DEFAULT_PROCS = 3                 # co dinh mac dinh 3, doi bang --procs
SEC_PER_CHUNK = 165.0             # throughput hieu dung do o 3 tien trinh (giay / chunk, tinh theo dong ho tuong)
ETA_REFRESH_EVERY = 10            # cap nhat uoc luong theo throughput thuc te sau moi 10 chunk
RAM_MIN_NEW_MB = 3072             # khong nhan chunk moi khi RAM kha dung < 3 GB
STAGGER_S = 45                    # tien trinh w doi w*45s truoc khi nhan chunk dau (tranh dung RAM dong thoi)
PHASE1_FILE = RES / "phase1_done.txt"
PARTIAL_KEY = B / "_answer_key" / "error_keys_100M_PARTIAL.csv"
PARTIAL_MISSING = B / "_answer_key" / "error_keys_100M_PARTIAL.missing.txt"
PHASE1 = list(range(N_EXISTING, NCHUNKS))
MAX_PROCS = 6
RAM_RESERVE_MB = 2048
PEAK_MB_STEP2 = 2048

ORDER = list(range(N_EXISTING, NCHUNKS)) + list(range(N_EXISTING))


# ----------------------------------------------------------------- tien ich he thong (Windows)
class _MEMSTAT(ctypes.Structure):
    _fields_ = [("dwLength", wintypes.DWORD), ("dwMemoryLoad", wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]


class _PMC(ctypes.Structure):
    _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]


def avail_mb():
    m = _MEMSTAT()
    m.dwLength = ctypes.sizeof(m)
    ctypes.WinDLL("kernel32").GlobalMemoryStatusEx(ctypes.byref(m))
    return m.ullAvailPhys / 2**20


def total_mb():
    m = _MEMSTAT()
    m.dwLength = ctypes.sizeof(m)
    ctypes.WinDLL("kernel32").GlobalMemoryStatusEx(ctypes.byref(m))
    return m.ullTotalPhys / 2**20


def peak_mb():
    k = ctypes.WinDLL("kernel32")
    p = ctypes.WinDLL("psapi")
    k.GetCurrentProcess.restype = wintypes.HANDLE
    p.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(_PMC), wintypes.DWORD]
    c = _PMC()
    c.cb = ctypes.sizeof(c)
    p.GetProcessMemoryInfo(k.GetCurrentProcess(), ctypes.byref(c), c.cb)
    return c.PeakWorkingSetSize / 2**20


def pid_alive(pid):
    k = ctypes.WinDLL("kernel32")
    k.OpenProcess.restype = wintypes.HANDLE
    k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    h = k.OpenProcess(0x1000, False, int(pid))   # PROCESS_QUERY_LIMITED_INFORMATION
    if not h:
        return False
    code = wintypes.DWORD()
    k.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    k.GetExitCodeProcess(h, ctypes.byref(code))
    k.CloseHandle.argtypes = [wintypes.HANDLE]
    k.CloseHandle(h)
    return code.value == 259   # STILL_ACTIVE


# ----------------------------------------------------------------- tien ich chung
def now_iso():
    return datetime.datetime.now().isoformat(timespec="seconds")


def log(msg):
    RES.mkdir(parents=True, exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"{now_iso()} [pid {os.getpid()}] {msg}\n")


def atomic_write_text(path, text):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def chunk_range(i):
    s = i * CHUNK
    return s, min(s + CHUNK, TARGET_MASTERS)


def part_path(t, i):
    return OUT / t / f"part-{i:05d}.parquet"


def key_path(i):
    return KEYS_PARTS / f"errors_{i:05d}.csv"


def done_path(i):
    return DONE / f"chunk_{i:05d}.json"


def read_done(i):
    try:
        return json.loads(done_path(i).read_text(encoding="utf-8"))
    except Exception:
        return None


def plan_workers(avail):
    return max(0, min(MAX_PROCS, int((avail - RAM_RESERVE_MB) / (PEAK_MB_STEP2 * 1.3))))


def fmt_dur(s):
    s = int(s)
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


# ----------------------------------------------------------------- so sanh cot (chunk 0..44)
def col_hash(chunked):
    import pandas as pd
    s = chunked.to_pandas()
    return hashlib.sha256(pd.util.hash_pandas_object(s, index=False).to_numpy().tobytes()).hexdigest()


def compare_with_existing(df, path):
    """Tra ve danh sach cot lech ([] neu giong het). Khong ghi gi len dia."""
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)     # cung ham ghi nhu save_chunk
    buf.seek(0)
    new = pq.read_table(buf)
    old = pq.read_table(path)
    if new.num_rows != old.num_rows:
        return [f"<num_rows {new.num_rows} vs {old.num_rows}>"]
    if new.schema.names != old.schema.names:
        return [f"<columns {new.schema.names} vs {old.schema.names}>"]
    bad = []
    for c in old.schema.names:
        if old.schema.field(c).type != new.schema.field(c).type:
            bad.append(f"{c}(type)")
        elif col_hash(old.column(c)) != col_hash(new.column(c)):
            bad.append(c)
    return bad


def write_key(i, err_df):
    KEYS_PARTS.mkdir(parents=True, exist_ok=True)
    p = key_path(i)
    tmp = p.with_name(p.name + ".tmp")
    err_df.to_csv(tmp, index=False)
    os.replace(tmp, p)


# ----------------------------------------------------------------- tien trinh con
def run_chunk(i, lengths, df_units, seed, run_id):
    from src.data_generator import generate_chunk
    s, e = chunk_range(i)
    t0 = time.perf_counter()
    master_df, detail_df, err_df, ml_labels, ml_anomaly = generate_chunk(
        start_master_idx=s, lengths=lengths[s:e], seed_offset=i,
        global_seed=seed, df_units=df_units)
    gen_s = time.perf_counter() - t0
    frames = {"MASTER": master_df, "DETAIL": detail_df, "ML_LABELS": ml_labels, "ML_ANOMALY": ml_anomaly}
    rows = {t: int(len(df)) for t, df in frames.items()}
    lech = {}
    key_written = False
    t1 = time.perf_counter()
    if i < N_EXISTING:
        mode = "verify"
        for t, df in frames.items():
            bad = compare_with_existing(df, part_path(t, i))
            if bad:
                lech[t] = bad
                log(f"LECH chunk={i} table={t} cols={bad}")
        if not lech:
            write_key(i, err_df)
            key_written = True
    else:
        mode = "new"
        for t, df in frames.items():
            (OUT / t).mkdir(parents=True, exist_ok=True)
            final = part_path(t, i)
            tmp = final.with_name(final.name + ".tmp")
            df.to_parquet(tmp, index=False)
            os.replace(tmp, final)
        write_key(i, err_df)
        key_written = True
        for t in TABLES:
            assert part_path(t, i).exists(), f"missing {part_path(t, i)}"
        assert key_path(i).exists()
    write_s = time.perf_counter() - t1
    DONE.mkdir(parents=True, exist_ok=True)
    marker = {"chunk": i, "mode": mode, "run_id": run_id, "rows": rows, "error_rows": int(len(err_df)),
              "key_written": key_written, "lech": lech, "gen_s": round(gen_s, 2),
              "write_s": round(write_s, 2), "total_s": round(gen_s + write_s, 2),
              "peak_mb": round(peak_mb(), 1), "pid": os.getpid(), "finished": now_iso()}
    atomic_write_text(done_path(i), json.dumps(marker, ensure_ascii=False))
    log(f"chunk={i} DONE mode={mode} gen_s={gen_s:.0f} write_s={write_s:.1f} "
        f"peak_mb={marker['peak_mb']} lech={'YES' if lech else 'no'} avail_mb={avail_mb():.0f}")


def worker_main(wid, lengths, run_id):
    try:
        from src.data_generator import config, generate_units_table
        seed = config["generation"]["global_seed"]
        t0 = time.perf_counter()
        df_units = generate_units_table(config["generation"]["num_units"][SCALE], seed)
        log(f"worker {wid} ready (units loaded once in {time.perf_counter() - t0:.1f}s, "
            f"avail_mb={avail_mb():.0f})")
        parent = mp.parent_process()
        waited = 0
        while waited < wid * STAGGER_S:
            if STOPF.exists() or (parent is not None and not parent.is_alive()):
                return
            time.sleep(5)
            waited += 5
        for i in ORDER:
            if STOPF.exists():
                log(f"worker {wid}: STOP flag, exiting")
                return
            if parent is not None and not parent.is_alive():
                log(f"worker {wid}: coordinator gone, exiting")
                return
            if done_path(i).exists():
                continue
            while avail_mb() < RAM_MIN_NEW_MB:
                if STOPF.exists() or (parent is not None and not parent.is_alive()):
                    return
                log(f"worker {wid}: avail RAM {avail_mb():.0f} MB < {RAM_MIN_NEW_MB}, waiting")
                time.sleep(30)
            try:
                CLAIMS.mkdir(parents=True, exist_ok=True)
                fd = os.open(str(CLAIMS / f"chunk_{i:05d}.claim"), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
            except FileExistsError:
                continue
            log(f"worker {wid}: chunk={i} START")
            try:
                run_chunk(i, lengths, df_units, seed, run_id)
            except Exception:
                tb = traceback.format_exc()
                log(f"chunk={i} FAILED\n{tb}")
                FAILED.mkdir(parents=True, exist_ok=True)
                (FAILED / f"chunk_{i:05d}.txt").write_text(tb, encoding="utf-8")
        log(f"worker {wid}: no more chunks, exiting")
    except Exception:
        log(f"worker {wid} CRASH\n{traceback.format_exc()}")


# ----------------------------------------------------------------- dieu phoi
def check_code_hashes():
    m = json.loads(MANIFEST.read_text(encoding="utf-8"))["code_hashes_sha256"]
    bad = []
    for rel, h in m.items():
        if sha256_file(ROOT / rel) != h:
            bad.append(rel)
    return bad


def dir_size_bytes(*dirs):
    tot = 0
    for d in dirs:
        for dp, _, fs in os.walk(d):
            for f in fs:
                try:
                    tot += os.path.getsize(os.path.join(dp, f))
                except OSError:
                    pass
    return tot


def count_this_run(run_id):
    n = 0
    for i in range(NCHUNKS):
        m = read_done(i)
        if m and m["run_id"] == run_id:
            n += 1
    return n


def write_status(state, procs, run_id, run_start, n_target, eff_s=SEC_PER_CHUNK, eff_src="uoc luong ban dau", extra=None):
    markers = [read_done(i) for i in range(NCHUNKS)]
    markers = [m for m in markers if m]
    done = len(markers)
    this_run = [m for m in markers if m["run_id"] == run_id]
    avg = (sum(m["total_s"] for m in this_run) / len(this_run)) if this_run else None
    alive = [p for p in procs if p.is_alive()]
    remaining = NCHUNKS - done
    eta = remaining * eff_s
    p1_done = sum(1 for m in markers if m["chunk"] >= N_EXISTING)
    p2_done = done - p1_done
    phase1 = PHASE1_FILE.read_text(encoding="utf-8").strip() if PHASE1_FILE.exists() else "giai doan 1 dang chay"
    st = {"state": state, "updated": now_iso(), "run_id": run_id, "run_started": run_start,
          "coordinator_pid": os.getpid(), "worker_pids": [p.pid for p in alive],
          "giai_doan_1": phase1, "phase1_done": f"{p1_done}/{len(PHASE1)}", "phase2_done": f"{p2_done}/{N_EXISTING}",
          "chunks_done": done, "chunks_total": NCHUNKS, "done_this_run": len(this_run),
          "procs_target": n_target, "procs_alive": len(alive),
          "avg_chunk_s_per_process": round(avg, 1) if avg else None,
          "effective_s_per_chunk": round(eff_s, 1), "effective_basis": eff_src,
          "eta_remaining": fmt_dur(eta) if alive else None,
          "eta_finish": (datetime.datetime.now() + datetime.timedelta(seconds=eta)).isoformat(timespec="minutes") if alive else None,
          "avail_ram_mb": round(avail_mb()), "output_gb": round(dir_size_bytes(OUT, KEYS_PARTS) / 2**30, 3),
          "lech_chunks": sorted(m["chunk"] for m in markers if m["lech"]),
          "failed_chunks": sorted(int(p.stem.split("_")[1]) for p in FAILED.glob("chunk_*.txt")) if FAILED.exists() else []}
    if extra:
        st.update(extra)
    atomic_write_text(STATUS, json.dumps(st, indent=1, ensure_ascii=False))


def merge_keys():
    missing = [i for i in range(NCHUNKS) if not key_path(i).exists()]
    if missing:
        return False, missing
    tmp = FINAL_KEY.with_name(FINAL_KEY.name + ".tmp")
    first_header = None
    with open(tmp, "wb") as out:
        for i in range(NCHUNKS):
            with open(key_path(i), "rb") as f:
                header = f.readline()
                if first_header is None:
                    first_header = header
                    out.write(header)
                elif header != first_header:
                    raise RuntimeError(f"header khac o chunk {i}")
                shutil.copyfileobj(f, out, 1 << 20)
    os.replace(tmp, FINAL_KEY)
    return True, []


def merge_partial():
    """Noi cac dap an loi dang co thanh error_keys_100M_PARTIAL.csv + file liet ke chunk thieu."""
    have = [i for i in range(NCHUNKS) if key_path(i).exists()]
    missing = [i for i in range(NCHUNKS) if i not in have]
    if have:
        tmp = PARTIAL_KEY.with_name(PARTIAL_KEY.name + ".tmp")
        first_header = None
        with open(tmp, "wb") as out:
            for i in have:
                with open(key_path(i), "rb") as f:
                    header = f.readline()
                    if first_header is None:
                        first_header = header
                        out.write(header)
                    elif header != first_header:
                        raise RuntimeError(f"header khac o chunk {i}")
                    shutil.copyfileobj(f, out, 1 << 20)
        os.replace(tmp, PARTIAL_KEY)
    atomic_write_text(PARTIAL_MISSING,
                      f"cap nhat: {now_iso()}\nco {len(have)}/{NCHUNKS} chunk dap an loi\n"
                      f"chunk con thieu ({len(missing)}): {missing}\n")
    log(f"PARTIAL: co {len(have)}/{NCHUNKS} chunk, thieu {len(missing)}: {missing}")
    return have, missing


def prepare(clean):
    """Chuan bi thu muc, kiem tra code, don dep. clean=True khi chay that."""
    for d in (RES, DONE, CLAIMS, FAILED, KEYS_PARTS):
        d.mkdir(parents=True, exist_ok=True)
    bad = check_code_hashes()
    if bad:
        return f"Code/config doi so voi manifest.json: {bad}. Dung."
    if STATUS.exists():
        try:
            old = json.loads(STATUS.read_text(encoding="utf-8"))
            live = [p for p in old.get("worker_pids", []) + [old.get("coordinator_pid", 0)]
                    if p and p != os.getpid() and pid_alive(p)]
            if live:
                return f"Van con tien trinh cu dang chay: {live}. Dung."
        except Exception:
            pass
    if not clean:
        return None
    for f in CLAIMS.glob("*.claim"):
        f.unlink()
    for f in FAILED.glob("*.txt"):
        f.unlink()
    if STOPF.exists():
        STOPF.unlink()
        log("xoa co STOP cu")
    for d in [*(OUT / t for t in TABLES), KEYS_PARTS, DONE]:
        if d.exists():
            for f in d.glob("*.tmp"):
                log(f"don file tam do dang: {f}")
                f.unlink()
    # dau hoan tat nhung thieu file -> bo dau
    for i in range(NCHUNKS):
        m = read_done(i)
        if not m:
            continue
        need = [key_path(i)] if m["key_written"] else []
        if i >= N_EXISTING:
            need += [part_path(t, i) for t in TABLES]
        if any(not p.exists() for p in need):
            log(f"chunk={i} co dau hoan tat nhung thieu file -> bo dau")
            done_path(i).unlink()
    if not SNAPSHOT.exists():
        snap = {}
        for t in TABLES:
            for i in range(N_EXISTING):
                p = part_path(t, i)
                stt = p.stat()
                snap[f"{t}/part-{i:05d}.parquet"] = {"size": stt.st_size, "mtime_ns": stt.st_mtime_ns}
        atomic_write_text(SNAPSHOT, json.dumps(snap))
        log(f"ghi snapshot {len(snap)} file chunk 0..44 (size, mtime)")
    return None


def make_plan():
    from src.data_generator import config, get_master_counts
    seed = config["generation"]["global_seed"]
    lengths = get_master_counts(TARGET_MASTERS, seed)
    pending = [i for i in ORDER if not done_path(i).exists()]
    avail = avail_mb()
    est = len(pending) * SEC_PER_CHUNK
    return lengths, pending, avail, est


def describe_plan(pending, avail, n, est, lengths):
    n_new = sum(1 for i in pending if i >= N_EXISTING)
    n_ver = len(pending) - n_new
    return (f"Ke hoach: tong chunk {NCHUNKS}, con lai {len(pending)} (giai doan 1 sinh moi {n_new}, "
            f"giai doan 2 sinh lai de lay dap an loi {n_ver}); tong dong DETAIL du kien {int(lengths.sum())}; "
            f"RAM kha dung {avail:.0f} MB / tong {total_mb():.0f} MB; so tien trinh = {n}; "
            f"nguong khong nhan chunk moi khi RAM kha dung < {RAM_MIN_NEW_MB} MB; "
            f"uoc luong = {len(pending)} chunk x {SEC_PER_CHUNK:.0f}s = {fmt_dur(est)}"
            + (f"  [CANH BAO: RAM kha dung hien < {RAM_MIN_NEW_MB} MB, tien trinh se cho]" if avail < RAM_MIN_NEW_MB else ""))


def cmd_dry_run(procs):
    err = prepare(clean=False)
    if err:
        print(err)
        return 2
    lengths, pending, avail, est = make_plan()
    print(describe_plan(pending, avail, procs, est, lengths))
    return 0


def check_phase1():
    if not PHASE1_FILE.exists() and all(done_path(i).exists() for i in PHASE1):
        msg = f"GIAI \u0110OAN 1 XONG {now_iso()}"
        atomic_write_text(PHASE1_FILE, msg)
        log(msg)
        merge_partial()


def cmd_run(n):
    mp.set_start_method("spawn")
    PIDF.parent.mkdir(parents=True, exist_ok=True)
    err = prepare(clean=True)
    if err:
        log(err)
        print(err)
        return 2
    PIDF.write_text(str(os.getpid()))
    run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_start = now_iso()
    import pandas, numpy, pyarrow
    log(f"=== RUN {run_id} start; python {sys.version.split()[0]} pandas {pandas.__version__} "
        f"numpy {numpy.__version__} pyarrow {pyarrow.__version__}")
    lengths, pending, avail, est = make_plan()
    log(describe_plan(pending, avail, n, est, lengths))
    if not pending:
        log("khong con chunk nao can chay")
    procs = [mp.Process(target=worker_main, args=(w, lengths, run_id), name=f"w{w}") for w in range(n)]
    for p in procs:
        p.start()
    log(f"da khoi dong {n} tien trinh: {[p.pid for p in procs]}")
    t_start = time.time()
    eff_s, eff_src, last_k = SEC_PER_CHUNK, "uoc luong ban dau 165s/chunk", 0
    while any(p.is_alive() for p in procs):
        nd = count_this_run(run_id)
        if nd // ETA_REFRESH_EVERY > last_k:
            last_k = nd // ETA_REFRESH_EVERY
            eff_s = (time.time() - t_start) / nd
            eff_src = f"throughput thuc te sau {nd} chunk cua lan chay nay"
            log(f"cap nhat uoc luong: {eff_s:.1f}s/chunk (hieu dung, {nd} chunk)")
        check_phase1()
        write_status("STOPPING" if STOPF.exists() else "RUNNING", procs, run_id, run_start, n, eff_s, eff_src)
        time.sleep(10)
    for p in procs:
        p.join()
    check_phase1()
    done = sum(1 for i in range(NCHUNKS) if done_path(i).exists())
    state, extra = None, {}
    if done == NCHUNKS:
        ok, missing = merge_keys()
        if ok:
            state = "FINISHED"
            log(f"da tao {FINAL_KEY}")
        else:
            state = "FINISHED_NO_KEY"
            log(f"khong tao duoc error_keys_100M.csv, thieu dap an loi cua chunk: {missing}")
    elif STOPF.exists():
        state = "STOPPED"
    else:
        state = "INCOMPLETE"
    if state != "FINISHED":
        have, missing = merge_partial()
        extra = {"partial_key_file": str(PARTIAL_KEY), "missing_key_chunks": missing}
    write_status(state, procs, run_id, run_start, n, eff_s, eff_src, extra)
    log(f"=== RUN {run_id} end state={state} done={done}/{NCHUNKS}")
    return 0


def cmd_launch(procs):
    err = prepare(clean=False)
    if err:
        print(err)
        return 2
    CREATE_NO_WINDOW, CREATE_NEW_PROCESS_GROUP, CREATE_BREAKAWAY_FROM_JOB = 0x08000000, 0x200, 0x01000000
    RES.mkdir(parents=True, exist_ok=True)
    out = open(STDOUT_LOG, "ab")
    try:
        p = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "run", "--procs", str(procs)], cwd=str(ROOT),
                             stdin=subprocess.DEVNULL, stdout=out, stderr=out, close_fds=True,
                             creationflags=CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB)
    except OSError as e:
        print(f"KHONG THE khoi chay tach roi tren may nay: {e!r}")
        return 3
    PIDF.write_text(str(p.pid))
    k = ctypes.WinDLL("kernel32")
    inj = wintypes.BOOL()
    k.IsProcessInJob.argtypes = [wintypes.HANDLE, wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL)]
    ok = k.IsProcessInJob(int(p._handle), None, ctypes.byref(inj))
    print(f"coordinator PID={p.pid}; IsProcessInJob_ok={bool(ok)} in_job={bool(inj.value)}")
    return 0


def cmd_status():
    if not STATUS.exists():
        print("chua co status.json")
        return 1
    print(STATUS.read_text(encoding="utf-8"))
    return 0


def cmd_stop():
    RES.mkdir(parents=True, exist_ok=True)
    STOPF.write_text(now_iso())
    print(f"da tao co dung {STOPF}; cac chunk dang chay se hoan tat roi thoat.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["dry-run", "launch", "status", "stop", "run"])
    ap.add_argument("--procs", type=int, default=DEFAULT_PROCS, help="so tien trinh song song (mac dinh 3)")
    a = ap.parse_args()
    if a.procs < 1:
        sys.exit("--procs phai >= 1")
    fn = {"dry-run": lambda: cmd_dry_run(a.procs), "launch": lambda: cmd_launch(a.procs),
          "status": cmd_status, "stop": cmd_stop, "run": lambda: cmd_run(a.procs)}[a.cmd]
    sys.exit(fn())
