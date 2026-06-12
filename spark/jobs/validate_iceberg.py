import sys

import great_expectations as gx

from utils import init_spark, validate_table, TABLE_NAMES


def main():
    spark = init_spark(app_name="GX-Validation")
    context = gx.get_context(mode="ephemeral")

    all_passed = True
    results_summary = []

    for full_name, short_name in TABLE_NAMES.items():
        print(f"\n{'='*50}")
        print(f"Validating {short_name} ({full_name})")
        print(f"{'='*50}")

        df = spark.table(full_name)
        row_count = df.count()
        print(f"  Rows: {row_count}")

        result = validate_table(context, spark, short_name, df)

        stats = result.statistics
        success = result.success
        all_passed = all_passed and success

        status = "PASSED" if success else "FAILED"
        print(f"  {status}")
        print(f"  Expectations: {stats['evaluated_expectations']} evaluated, "
              f"{stats['successful_expectations']} passed, "
              f"{stats['unsuccessful_expectations']} failed "
              f"({stats['success_percent']}%)")

        if not success:
            for r in result.results:
                if not r.success:
                    print(f"    FAILED: {r.expectation_config.type} on column "
                          f"{r.expectation_config.kwargs.get('column', '?')}")

        results_summary.append((short_name, status, row_count))

    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    for name, status, count in results_summary:
        print(f"  {name:15s} {status:6s} ({count} rows)")

    spark.stop()

    if not all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
