"""Checks that the split test jobs together ran every JUnit test method of the sources exactly once.

usage: python -I scripts/check-test-split.py <reports-dir> <test-source-root>...

<reports-dir> holds one sub-directory per test job (the job's surefire reports, TEST-*.xml, anywhere below it). Every method annotated
@Test or @ParameterizedTest under the source roots must have run in exactly one job, and no job may report a method the sources do not
have. Exit 0 when that holds, 1 otherwise; it prints the totals either way.
"""
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

ANNOTATED = re.compile(r"@(?:Test|ParameterizedTest)\b(?:\([^)]*\))?(?:\s+@\w+(?:\([^)]*\))?)*\s+(?:(?:public|protected|private|static|final)\s+)*void\s+(\w+)\s*\(")


def declared(roots):
    methods = set()
    for root in roots:
        for f in pathlib.Path(root).rglob("*.java"):
            text = f.read_text(encoding="utf-8")
            pkg = re.search(r"^package\s+([\w.]+);", text, re.M)
            cls = (pkg.group(1) + "." if pkg else "") + f.stem
            for m in ANNOTATED.finditer(text):
                methods.add((cls, m.group(1)))
    return methods


def ran(reports):
    jobs, cases, skipped = {}, 0, 0
    for job in sorted(p for p in pathlib.Path(reports).iterdir() if p.is_dir()):
        for x in job.rglob("TEST-*.xml"):
            for tc in ET.parse(x).getroot().iter("testcase"):
                cases += 1
                if tc.find("skipped") is not None:
                    skipped += 1
                name = re.split(r"[(\[]", tc.get("name"), maxsplit=1)[0].strip()
                jobs.setdefault((tc.get("classname"), name), set()).add(job.name)
    return jobs, cases, skipped


def main(argv):
    if len(argv) < 3:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    want = declared(argv[2:])
    jobs, cases, skipped = ran(argv[1])
    problems = [f"not run: {c}#{m}" for c, m in sorted(want - jobs.keys())]
    problems += [f"run but not declared: {c}#{m}" for c, m in sorted(jobs.keys() - want)]
    problems += [f"run in {len(j)} jobs ({', '.join(sorted(j))}): {c}#{m}" for (c, m), j in sorted(jobs.items()) if len(j) > 1]
    print(f"declared methods {len(want)}, run methods {len(jobs)}, test cases {cases} (skipped {skipped})")
    for p in problems:
        print(p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
