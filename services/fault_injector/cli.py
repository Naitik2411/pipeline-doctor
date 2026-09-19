from __future__ import annotations

import argparse
from datetime import date

from services.fault_injector import injectors


def main() -> None:
    p = argparse.ArgumentParser(description="PipelineDoctor fault injector")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_day(sp):
        sp.add_argument("--day", type=date.fromisoformat, default=None,
                        help="YYYY-MM-DD in business timezone (default: yesterday)")

    sp = sub.add_parser("missing-batch", help="Class 1")
    add_day(sp)

    sp = sub.add_parser("duplicates", help="Class 2")
    add_day(sp)

    sub.add_parser("schema-drift", help="Class 3")
    sub.add_parser("timezone-bug", help="Class 4")

    sp = sub.add_parser("business-change", help="Class 5 control")
    add_day(sp)
    sp.add_argument("--drop-fraction", type=float, default=0.4)

    sp = sub.add_parser("reset", help="Clear fault state + optional reseed")
    sp.add_argument("--no-reseed", action="store_true")

    sub.add_parser("status", help="Show active fault state")

    args = p.parse_args()

    if args.cmd == "missing-batch":
        print(injectors.inject_missing_batch(args.day))
    elif args.cmd == "duplicates":
        print(injectors.inject_duplicates(args.day))
    elif args.cmd == "schema-drift":
        print(injectors.inject_schema_drift())
    elif args.cmd == "timezone-bug":
        print(injectors.inject_timezone_bug())
    elif args.cmd == "business-change":
        print(injectors.inject_business_change(args.day, args.drop_fraction))
    elif args.cmd == "reset":
        print(injectors.reset_faults(reseed=not args.no_reseed))
    elif args.cmd == "status":
        from services.fault_injector.state import read_state
        print(read_state())


if __name__ == "__main__":
    main()