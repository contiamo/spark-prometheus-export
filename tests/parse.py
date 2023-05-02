import sys
from prometheus_client.parser import text_string_to_metric_families

input = sys.stdin.read()
families = text_string_to_metric_families(input)

for family in families:
    print(family)
    for sample in family.samples:
        print(sample)