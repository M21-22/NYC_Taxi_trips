"""Customer field names and local synthetic enrichment."""

from collections.abc import Iterator

CUSTOMER_FIELDS = ("customer_first_name", "customer_last_name", "customer_email")


def customer_rows(seed: int, count: int) -> Iterator[tuple[int, str, str, str]]:
    from faker import Faker

    fake = Faker("en_US")
    fake.seed_instance(seed)
    for customer_id in range(1, count + 1):
        yield (
            customer_id,
            fake.first_name(),
            fake.last_name(),
            f"customer-{customer_id}@example.com",
        )
