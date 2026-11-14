#!/usr/bin/env python3
"""Validate the implementation ledger and report build-ready tickets."""

import argparse
from collections import Counter
from graphlib import CycleError, TopologicalSorter
import json
from pathlib import Path
import re
import sys
import unittest


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LEDGER = ROOT / "docs/implementation/backlog.json"
DEFAULT_ROADMAP = ROOT / "docs/implementation/roadmap.md"
TICKET_ID = r"(?:C[0-9]{2}|[AP][0-9]{2}-T[0-9]{2})"
STATUSES = (
    "proposed", "in_progress", "blocked", "keep", "component_done", "integrated", "qualified"
)
BUILD_COMPLETE = frozenset({"keep", "component_done", "integrated", "qualified"})
INTEGRATED = frozenset({"integrated", "qualified"})
ROADMAP_FIELDS = ("id", "owns", "build_after", "integrate_after", "conditional_dependencies")

# These numbered steps describe additional, capability-specific conditions in prose.
CONDITIONAL_STEPS = {
    "A02-T02": (3,),
    "P03-T20": (4,),
    "P05-T05": (3,),
    "P05-T06": (2,),
    "P06-T04": (3, 4),
    "P06-T07": (1,),
    "P07-T07": (4,),
    "P14-T09": (3,),
}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def check_fields(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ValueError(f"{label} must contain exactly: {', '.join(fields)}")


def check_string(value, label):
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{label} must be a nonempty string without surrounding whitespace")


def check_list(value, label):
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")


def check_ids(value, label):
    check_list(value, label)
    seen = set()
    for item in value:
        if not isinstance(item, str) or re.fullmatch(TICKET_ID, item) is None:
            raise ValueError(f"{label} contains an invalid ticket ID")
        if item in seen:
            raise ValueError(f"{label} contains duplicate ID {item}")
        seen.add(item)


def check_paths(value, label):
    check_list(value, label)
    seen = set()
    for path in value:
        parts = path_parts(path, label)
        if parts in seen:
            raise ValueError(f"{label} contains duplicate path {path}")
        seen.add(parts)


def path_parts(value, label):
    check_string(value, label)
    parts = value.split("/")
    if re.search(r'[\\:*?"<>|\x00-\x1f]', value) or any(
        not part or part.endswith((" ", ".")) for part in parts
    ):
        raise ValueError(f"{label} must use a relative repository path: {value!r}")
    # Compare case-insensitively so reservations also work on Windows.
    return tuple(part.casefold() for part in parts)


def parse_roadmap(text):
    headings = list(re.finditer(rf"^#### ({TICKET_ID}) — .+$", text, re.MULTILINE))
    count = re.search(r"^\*\*Backlog size:\*\* ([0-9]+) tickets\.", text, re.MULTILINE)
    if not count or len(headings) != int(count[1]):
        raise ValueError("roadmap ticket headings do not match the declared backlog size")

    def dependencies(field):
        if field == "None":
            return []
        ids = []
        for token in field.split(", "):
            link = re.fullmatch(rf"\[({TICKET_ID})\]\(#ticket-([a-z0-9-]+)\)", token)
            if not link or link[1].lower() != link[2]:
                raise ValueError(f"invalid roadmap dependency link: {token}")
            ids.append(link[1])
        return ids

    tickets = []
    for index, heading in enumerate(headings):
        ticket_id = heading[1]
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        body = text[heading.end():end]
        owns = re.search(r"^\*\*Owns:\*\* (.+?)\. \*\*Validation:\*\*", body, re.MULTILINE)
        edges = re.search(
            r"^\*\*Build after:\*\* (.+?)\. \*\*Integrate after:\*\* (.+)\.$", body, re.MULTILINE
        )
        if not owns or not edges:
            raise ValueError(f"{ticket_id} lacks a roadmap ownership or dependency row")
        conditional = []
        for number in CONDITIONAL_STEPS.get(ticket_id, ()):
            step = re.search(rf"^{number}\. (.+)$", body, re.MULTILINE)
            if not step:
                raise ValueError(f"{ticket_id} lacks conditional step {number}")
            condition = step[1]
            ids = dict.fromkeys(re.findall(rf"\b{TICKET_ID}\b", condition))
            conditional.append({"condition": condition, "after": list(ids)})
        tickets.append({
            "id": ticket_id,
            "owns": owns[1],
            "build_after": dependencies(edges[1]),
            "integrate_after": dependencies(edges[2]),
            "conditional_dependencies": conditional,
        })
    return tickets


def compare_roadmap(tickets, roadmap_path):
    expected = parse_roadmap(roadmap_path.read_text(encoding="utf-8-sig"))
    if [ticket["id"] for ticket in tickets] != [ticket["id"] for ticket in expected]:
        raise ValueError("ledger ticket IDs or order differ from the roadmap")
    for actual, reference in zip(tickets, expected):
        for field in ROADMAP_FIELDS[1:]:
            if actual[field] != reference[field]:
                raise ValueError(f"{actual['id']} ledger field {field} differs from the roadmap")


def validate_ledger(ledger, evidence_root=None):
    check_fields(ledger, ("schema_version", "tickets", "active_reservations"), "ledger")
    if type(ledger["schema_version"]) is not int or ledger["schema_version"] != 1:
        raise ValueError("schema_version must be the integer 1")

    tickets = ledger["tickets"]
    check_list(tickets, "tickets")
    if not tickets:
        raise ValueError("tickets must contain at least one ticket")
    by_id = {}
    for ticket in tickets:
        check_fields(ticket, (*ROADMAP_FIELDS, "status", "evidence"), "ticket")
        ticket_id = ticket["id"]
        check_ids([ticket_id], "ticket id")
        if ticket_id in by_id:
            raise ValueError(f"duplicate ticket: {ticket_id}")
        by_id[ticket_id] = ticket
        check_string(ticket["owns"], f"{ticket_id} owns")
        for field in ("build_after", "integrate_after"):
            check_ids(ticket[field], f"{ticket_id} {field}")
        conditional = ticket["conditional_dependencies"]
        check_list(conditional, f"{ticket_id} conditional_dependencies")
        for gate in conditional:
            check_fields(gate, ("condition", "after"), f"{ticket_id} conditional dependency")
            check_string(gate["condition"], f"{ticket_id} condition")
            check_ids(gate["after"], f"{ticket_id} conditional after")
        status = ticket["status"]
        if not isinstance(status, str) or status not in STATUSES:
            raise ValueError(f"{ticket_id} has invalid status")
        evidence = ticket["evidence"]
        check_paths(evidence, f"{ticket_id} evidence")
        if status in BUILD_COMPLETE and not evidence:
            raise ValueError(f"{ticket_id} status {status} requires evidence")
        if evidence_root is not None:
            for path in evidence:
                if not (evidence_root / path).is_file():
                    raise ValueError(f"{ticket_id} evidence file is missing: {path}")

    for ticket_id, ticket in by_id.items():
        unconditional = ticket["build_after"] + ticket["integrate_after"]
        conditional = [dep for gate in ticket["conditional_dependencies"] for dep in gate["after"]]
        for dependency in unconditional + conditional:
            if dependency not in by_id:
                raise ValueError(f"{ticket_id} has unknown dependency {dependency}")
            if dependency == ticket_id:
                raise ValueError(f"{ticket_id} depends on itself")

    # Integration can depend on a build, while a build never waits for integration.
    for stage, field in (("build", "build_after"), ("integration", "integrate_after")):
        graph = {ticket_id: ticket[field] for ticket_id, ticket in by_id.items()}
        try:
            TopologicalSorter(graph).prepare()
        except CycleError as error:
            cycle = " -> ".join(error.args[1])
            raise ValueError(f"{stage} dependency cycle: {cycle}") from error

    for ticket_id, ticket in by_id.items():
        status = ticket["status"]
        if status in BUILD_COMPLETE:
            for dependency in ticket["build_after"]:
                if by_id[dependency]["status"] not in BUILD_COMPLETE:
                    raise ValueError(f"{ticket_id} needs build-complete dependency {dependency}")
        if status in INTEGRATED:
            for dependency in ticket["integrate_after"]:
                if by_id[dependency]["status"] not in INTEGRATED:
                    raise ValueError(f"{ticket_id} needs integrated dependency {dependency}")

    reservations = ledger["active_reservations"]
    check_list(reservations, "active_reservations")
    active = []
    for reservation in reservations:
        check_fields(reservation, ("ticket", "path"), "active reservation")
        ticket_id = reservation["ticket"]
        check_ids([ticket_id], "reservation ticket")
        if ticket_id not in by_id:
            raise ValueError(f"reservation refers to unknown ticket {ticket_id}")
        path = reservation["path"]
        parts = path_parts(path, f"{ticket_id} reservation")
        for other_parts, other_ticket, other_path in active:
            common = min(len(parts), len(other_parts))
            if parts[:common] == other_parts[:common]:
                raise ValueError(
                    f"overlapping active reservations: {other_ticket} {other_path} and "
                    f"{ticket_id} {path}"
                )
        active.append((parts, ticket_id, path))

    return by_id


def build_ready(by_id):
    return [
        ticket_id for ticket_id, ticket in by_id.items()
        if ticket["status"] == "proposed" and all(
            by_id[dependency]["status"] in BUILD_COMPLETE for dependency in ticket["build_after"]
        )
    ]


class LedgerTests(unittest.TestCase):
    @staticmethod
    def ticket(ticket_id, *, build=(), integrate=(), status="proposed"):
        return {
            "id": ticket_id,
            "owns": "shared file",
            "build_after": list(build),
            "integrate_after": list(integrate),
            "conditional_dependencies": [],
            "status": status,
            "evidence": ["proof.md"] if status in BUILD_COMPLETE else [],
        }

    @staticmethod
    def ledger(*tickets, reservations=()):
        return {"schema_version": 1, "tickets": list(tickets), "active_reservations": list(reservations)}

    def test_build_cycle_fails(self):
        ledger = self.ledger(self.ticket("C01", build=["C02"]), self.ticket("C02", build=["C01"]))
        with self.assertRaisesRegex(ValueError, "build dependency cycle"):
            validate_ledger(ledger)

    def test_integration_cycle_fails(self):
        ledger = self.ledger(
            self.ticket("C01", integrate=["C02"]), self.ticket("C02", integrate=["C01"])
        )
        with self.assertRaisesRegex(ValueError, "integration dependency cycle"):
            validate_ledger(ledger)

    def test_build_and_integration_edges_can_form_a_valid_staged_sequence(self):
        first_integration = self.ticket("C01", build=["C02"])
        first_build = self.ticket("C02", integrate=["C01"])
        ledger = self.ledger(first_integration, first_build)

        self.assertEqual(build_ready(validate_ledger(ledger)), ["C02"])
        first_build.update(status="component_done", evidence=["proof.md"])
        self.assertEqual(build_ready(validate_ledger(ledger)), ["C01"])
        first_integration.update(status="integrated", evidence=["proof.md"])
        first_build["status"] = "integrated"
        self.assertEqual(build_ready(validate_ledger(ledger)), [])

    def test_missing_dependency_fails(self):
        for field in ("build", "integrate"):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "unknown dependency"):
                validate_ledger(self.ledger(self.ticket("C01", **{field: ["C02"]})))

    def test_blocked_work_does_not_hide_unrelated_ready_tickets(self):
        ledger = self.ledger(
            self.ticket("C03"), self.ticket("C02", build=["C01"]),
            self.ticket("C01", status="blocked"), self.ticket("C04"),
        )
        self.assertEqual(build_ready(validate_ledger(ledger)), ["C03", "C04"])

    def test_component_done_unblocks_build_only(self):
        component = self.ticket("C01", status="component_done")
        consumer = self.ticket("C02", build=["C01"], integrate=["C01"])
        by_id = validate_ledger(self.ledger(component, consumer))
        self.assertEqual(build_ready(by_id), ["C02"])
        self.assertEqual(by_id["C01"]["status"], "component_done")
        for status in ("integrated", "qualified"):
            consumer.update(status=status, evidence=["proof.md"])
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, "needs integrated"):
                validate_ledger(self.ledger(component, consumer))
        component["status"] = "integrated"
        self.assertEqual(validate_ledger(self.ledger(component, consumer))["C02"]["status"], "qualified")

    def test_active_reservations_cover_exact_paths_and_subtrees(self):
        tickets = (self.ticket("C01"), self.ticket("C02"))
        first = {"ticket": "C01", "path": "crates/Core"}
        for path in ("crates/core", "crates/core/src/lib.rs"):
            second = {"ticket": "C02", "path": path}
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "overlapping active"):
                validate_ledger(self.ledger(*tickets, reservations=[first, second]))
        second = {"ticket": "C02", "path": "crates/core2"}
        self.assertEqual(build_ready(validate_ledger(self.ledger(*tickets, reservations=[first, second]))),
                         ["C01", "C02"])

    def test_conditional_dependency_does_not_gate_base_build(self):
        base = self.ticket("C01")
        base["conditional_dependencies"] = [{"condition": "enable optional mode", "after": ["C02"]}]
        optional = self.ticket("C02", build=["C01"])
        self.assertEqual(build_ready(validate_ledger(self.ledger(base, optional))), ["C01"])
        with self.assertRaisesRegex(ValueError, "unknown dependency"):
            validate_ledger(self.ledger(base))

    def test_invalid_schema_and_paths_fail(self):
        with self.assertRaisesRegex(ValueError, "schema_version"):
            validate_ledger({"schema_version": True, "tickets": [], "active_reservations": []})
        for path in ("../outside", "C:/absolute", "crates\\core", "crates//core"):
            reservation = {"ticket": "C01", "path": path}
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "relative repository path"):
                validate_ledger(self.ledger(self.ticket("C01"), reservations=[reservation]))
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            json.loads('{"status":"proposed","status":"qualified"}', object_pairs_hook=unique_object)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument(
        "--roadmap",
        type=Path,
        help="verify roadmap fields against this Markdown file (defaults to docs/implementation/roadmap.md for the default ledger)",
    )
    parser.add_argument("--self-test", action="store_true", help="run the checker fixtures")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(LedgerTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1

    roadmap = args.roadmap
    if roadmap is None and args.ledger.resolve() == DEFAULT_LEDGER.resolve():
        roadmap = DEFAULT_ROADMAP

    try:
        ledger = json.loads(args.ledger.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
        by_id = validate_ledger(ledger, evidence_root=ROOT)
        if roadmap is not None:
            compare_roadmap(ledger["tickets"], roadmap)
    except (OSError, ValueError) as error:
        print(f"backlog: {error}", file=sys.stderr)
        return 1

    counts = Counter(ticket["status"] for ticket in by_id.values())
    summary = ", ".join(f"{status}={counts[status]}" for status in STATUSES)
    print(f"{len(by_id)} tickets ({summary})")
    if roadmap is not None:
        print("Roadmap IDs, order, ownership, and dependency fields match.")
    ready = build_ready(by_id)
    print(f"Build-ready ({len(ready)}):")
    for ticket_id in ready:
        print(ticket_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
