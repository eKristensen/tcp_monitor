# Verbatim excerpt of Microsoft's all.txt: only deletes lines, never edits.
# Keeps the file header, the whole top-level sections named in SECTIONS, and
# the individual guidelines named in IDS (with their section heading).
# Code fences are tracked so "# ..." lines inside code are not headings.
BEGIN {
  split("AI Guidelines|Application Guidelines|Correctness Guidelines|Documentation|Project Guidelines|Universal Guidelines", s, "|")
  for (i in s) sections["# " s[i]] = 1
  split("M-INTEGRATION-TESTS M-LOG-NOT-PRINT M-MOCKABLE-SYSCALLS", d, " ")
  for (i in d) ids[d[i]] = 1
  keep = 1
}
/^```/ { fence = !fence }
!fence && /^# / && NR > 3 {
  section_keep = ($0 in sections)
  keep = section_keep
  pending = section_keep ? "" : $0
}
!fence && /^## / && NR > 3 {
  keep = section_keep
  if (!keep && match($0, /\(M-[A-Z-]+\)/) && (substr($0, RSTART + 1, RLENGTH - 2) in ids)) {
    keep = 1
    if (pending != "") { print pending; print ""; pending = "" }
  }
}
keep { print }
