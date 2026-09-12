from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from faker import Faker

from services.mock_api.config import settings
from services.mock_api.models import Order

STATUSES = ["paid", "paid", "paid", "refunded", "cancelled"]


def make_faker(seed: int | None = None) -> Faker:
    fake = Faker()
    fake.seed_instance(seed if seed is not None else settings.faker_seed)
    return fake


def generate_orders_for_day(
    day: date,
    n: int,
    fake: Faker | None = None,
    tz_name: str | None = None,
) -> list[Order]:
    """Generate n orders for a local calendar day (business timezone)."""
    fake = fake or make_faker()
    tz = ZoneInfo(tz_name or settings.business_timezone)
    orders: list[Order] = []

    for _ in range(n):
        # Spread across the local day — needed later for timezone-bug histograms
        local_dt = datetime.combine(day, time(0, 0), tzinfo=tz) + timedelta(
            seconds=fake.random_int(0, 24 * 60 * 60 - 1)
        )
        orders.append(
            Order(
                order_id=f"ord_{fake.unique.bothify(text='########')}",
                customer_id=f"cust_{fake.random_int(1, 50_000)}",
                order_total=Decimal(
                    str(round(fake.pyfloat(min_value=5, max_value=500, right_digits=2), 2))
                ),
                currency="USD",
                status=fake.random_element(STATUSES),
                ordered_at=local_dt,
            )
        )
    return orders


def generate_history(
    days: int,
    orders_per_day: int,
    end_day: date | None = None,
) -> list[Order]:
    fake = make_faker()
    end = end_day or date.today()
    start = end - timedelta(days=days - 1)

    all_orders: list[Order] = []
    day = start
    while day <= end:
        all_orders.extend(generate_orders_for_day(day, orders_per_day, fake=fake))
        day += timedelta(days=1)
    return all_orders
