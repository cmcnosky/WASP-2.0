# Plan-only baseline for the paper environment.
#
# Applying this from development or CI is forbidden by the house rules. An operator
# applies it after authorizing an AWS account and environment.

resource "null_resource" "paper_baseline" {}

resource "null_resource" "observer" {}
