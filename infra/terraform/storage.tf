resource "aws_s3_bucket" "data" {
  bucket              = "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-data"
  force_destroy       = false
  object_lock_enabled = true

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-data"
    Purpose = "immutable-market-and-release-data"
  })
}

resource "aws_s3_bucket" "audit" {
  bucket              = "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-audit"
  force_destroy       = false
  object_lock_enabled = true

  tags = merge(local.common_tags, {
    Name    = "${local.name_prefix}-audit"
    Purpose = "immutable-audit-exports"
  })
}

resource "aws_s3_bucket_versioning" "protected" {
  for_each = {
    data  = aws_s3_bucket.data.id
    audit = aws_s3_bucket.audit.id
  }

  bucket = each.value
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "protected" {
  for_each = {
    data  = aws_s3_bucket.data.id
    audit = aws_s3_bucket.audit.id
  }

  bucket = each.value
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.main.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "protected" {
  for_each = {
    data  = aws_s3_bucket.data.id
    audit = aws_s3_bucket.audit.id
  }

  bucket = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_object_lock_configuration" "protected" {
  for_each = {
    data  = aws_s3_bucket.data.id
    audit = aws_s3_bucket.audit.id
  }

  bucket = each.value

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = local.object_retention_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.protected]
}

resource "aws_s3_bucket_lifecycle_configuration" "protected" {
  for_each = {
    data  = aws_s3_bucket.data.id
    audit = aws_s3_bucket.audit.id
  }

  bucket = each.value

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.protected]
}

data "aws_iam_policy_document" "bucket_tls" {
  for_each = {
    data  = aws_s3_bucket.data.arn
    audit = aws_s3_bucket.audit.arn
  }

  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]
    resources = [
      each.value,
      "${each.value}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "protected" {
  for_each = {
    data = {
      bucket = aws_s3_bucket.data.id
      policy = data.aws_iam_policy_document.bucket_tls["data"].json
    }
    audit = {
      bucket = aws_s3_bucket.audit.id
      policy = data.aws_iam_policy_document.bucket_tls["audit"].json
    }
  }

  bucket = each.value.bucket
  policy = each.value.policy

  depends_on = [aws_s3_bucket_public_access_block.protected]
}

resource "aws_ecr_repository" "app" {
  name                 = local.name_prefix
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.main.arn
  }

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = merge(local.common_tags, { Name = local.name_prefix })
}

resource "aws_ecr_repository_policy" "app" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyUnencryptedTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "ecr:*"
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })
}
