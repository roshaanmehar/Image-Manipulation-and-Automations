#!/usr/bin/env python3
import argparse, json, os, sys, hashlib, random
from pathlib import Path
from datetime import datetime

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".webp", ".tif", ".tiff", ".bmp", ".dng"}

def sha256_of(path, chunk=1024*1024):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def collect(src: Path, image_only=True, full_hash=False, sample_hash=0):
    src = src.resolve()
    files = []
    # build a list first so we can sample deterministically
    all_paths = []
    for root, _, names in os.walk(src):
        for n in names:
            p = Path(root) / n
            if image_only and p.suffix.lower() not in IMAGE_EXTS:
                continue
            all_paths.append(p)

    # decide which to hash
    to_hash = set()
    if full_hash:
        to_hash = set(all_paths)
    elif sample_hash and sample_hash > 0:
        # sample without replacement
        k = min(sample_hash, len(all_paths))
        to_hash = set(random.sample(all_paths, k))

    for p in all_paths:
        st = p.stat()
        item = {
            "rel": str(p.relative_to(src)),
            "size": st.st_size,
            "mtime_ns": st.st_mtime_ns,  # very precise; avoids false positives
        }
        if p in to_hash:
            item["sha256"] = sha256_of(p)
        files.append(item)

    return {
        "scanned_at": datetime.utcnow().isoformat() + "Z",
        "root": str(src),
        "image_only": image_only,
        "full_hash": full_hash,
        "sample_hashed": len(to_hash),
        "files": files,
    }

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def index_by_rel(data):
    return {f["rel"]: f for f in data["files"]}

def compare(baseline, current):
    base_idx = index_by_rel(baseline)
    curr_idx = index_by_rel(current)

    base_set = set(base_idx.keys())
    curr_set = set(curr_idx.keys())

    missing = sorted(base_set - curr_set)   # existed before, not now
    added   = sorted(curr_set - base_set)   # new now (should be 0 for source)

    changed = []
    # compare common files
    for rel in sorted(base_set & curr_set):
        b = base_idx[rel]
        c = curr_idx[rel]
        # primary check: size + mtime_ns
        size_diff = (b["size"] != c["size"])
        mtime_diff = (b["mtime_ns"] != c["mtime_ns"])
        hash_diff = False
        if "sha256" in b and "sha256" in c:
            hash_diff = (b["sha256"] != c["sha256"])
        # mark changed if any primary diff or (if we had hashes) hash differs
        if size_diff or mtime_diff or hash_diff:
            changed.append({
                "rel": rel,
                "size_before": b["size"], "size_now": c["size"],
                "mtime_before": b["mtime_ns"], "mtime_now": c["mtime_ns"],
                "hash_before": b.get("sha256"), "hash_now": c.get("sha256"),
            })

    return missing, added, changed

def main():
    ap = argparse.ArgumentParser(description="Verify source folder integrity: baseline and compare.")
    ap.add_argument("--src", required=True, help="Source root to verify (your ORIGINALS).")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write-baseline", help="Write baseline JSON to this path and exit.")
    mode.add_argument("--compare", help="Compare current state to baseline JSON at this path.")
    ap.add_argument("--all-files", action="store_true", help="Include all files (not just images).")
    ap.add_argument("--full-hash", action="store_true", help="Hash every file (slower, strongest check).")
    ap.add_argument("--sample-hash", type=int, default=0, help="Hash N random files for extra confidence.")
    ap.add_argument("--out-report", help="Optional CSV to write detailed differences.")
    args = ap.parse_args()

    src = Path(args.src).expanduser().resolve()
    if not src.is_dir():
        print(f"ERROR: Source not found: {src}", file=sys.stderr); sys.exit(1)

    if args.write_baseline:
        data = collect(src, image_only=not args.all_files, full_hash=args.full_hash, sample_hash=args.sample_hash)
        Path(args.write_baseline).parent.mkdir(parents=True, exist_ok=True)
        with open(args.write_baseline, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"Baseline written: {args.write_baseline}")
        print(f"Files indexed: {len(data['files'])} | sample_hashed={data['sample_hashed']} | full_hash={data['full_hash']}")
        return

    # compare mode
    baseline = load_json(args.compare)
    current = collect(src, image_only=baseline.get("image_only", True),
                      full_hash=baseline.get("full_hash", False),
                      sample_hash=0)  # we only hash if baseline did (or you can rerun with --full-hash)
    missing, added, changed = compare(baseline, current)

    print("Integrity check summary")
    print("-----------------------")
    print(f"Source:       {src}")
    print(f"Files before: {len(baseline['files'])}")
    print(f"Files now:    {len(current['files'])}")
    print(f"Missing:      {len(missing)}")
    print(f"Added:        {len(added)}")
    print(f"Changed:      {len(changed)}")

    if args.out_report:
        import csv
        with open(args.out_report, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["type","relative_path","size_before","size_now","mtime_before","mtime_now","hash_before","hash_now"])
            for rel in missing:
                w.writerow(["missing", rel, "", "", "", "", "", ""])
            for rel in added:
                w.writerow(["added", rel, "", "", "", "", "", ""])
            for ch in changed:
                w.writerow(["changed", ch["rel"], ch["size_before"], ch["size_now"],
                            ch["mtime_before"], ch["mtime_now"], ch["hash_before"] or "", ch["hash_now"] or ""])
        print(f"Report written: {args.out_report}")

    # Non-zero exit if anything unexpected
    if missing or added or changed:
        sys.exit(2)

if __name__ == "__main__":
    main()
